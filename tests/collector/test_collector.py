import tempfile
import unittest
from pathlib import Path

from hangar_ingest.runs.database import Database
from hangar_ingest.quality import devices, rules, review
from hangar_ingest.runs import inspection, journal
from hangar_ingest.collector.pipeline import Collector, batch_id_for

HEADER = b'event_id,device,observed_at,value,unit\n'


class CollectorCase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.logs = Path(self.directory.name) / 'logs'
        self.logs.mkdir()
        self.db_path = Path(self.directory.name) / 'p.db'
        self.db = Database(self.db_path)
        devices.register(self.db, 'dev1', 'plane1', 'engine1', 'temperature', 'C')
        devices.add_alias(
            self.db, 'feed1', 'sensor1', 'dev1',
            '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
        )
        devices.add_calibration(
            self.db, 'cal1', 'dev1',
            '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
        )
        rules.configure(self.db, 'dev1', minimum='-100', maximum='200')
        self.collector = Collector(self.db, 'feed1', self.logs, name='hangar')

    def tearDown(self):
        self.db.close()
        self.directory.cleanup()

    def write(self, name, data):
        (self.logs / name).write_bytes(data)

    def append(self, name, data):
        with (self.logs / name).open('ab') as handle:
            handle.write(data)

    def events(self):
        return {row['external_event'] for row in self.db.all('SELECT external_event FROM observations')}


class RotationDrillTests(CollectorCase):
    def drill(self):
        # start：表头 + 完整 e1 + 半条 e2
        self.write('sensor.csv', HEADER + b'e1,sensor1,2026-11-11T10:00:01Z,20,C\n'
                   b'e2,sensor1,2026-11-11T10:00:02Z,"2')
        first = self.collector.run_once()
        self.assertEqual(first['records'], 1)
        state = self.collector.positions()[0]
        # 读取高水位覆盖整个文件；确认位置停在半条 e2 的起点，半条留在 pending
        self.assertEqual(state['read_position'], len((self.logs / 'sensor.csv').read_bytes()))
        self.assertEqual(state['position'] + len(state['pending']), state['read_position'])
        self.assertEqual(state['pending'], b'e2,sensor1,2026-11-11T10:00:02Z,"2')

        # rotate：补全 e2，旧文件改名，新文件带表头 + e3
        self.append('sensor.csv', b'1",C\n')
        old_path = self.logs / 'sensor.csv'
        old_inode = old_path.stat().st_ino
        old_path.rename(self.logs / 'sensor.1.csv')
        self.write('sensor.csv', HEADER + b'e3,sensor1,2026-11-11T10:00:03Z,22,C\n')
        second = self.collector.run_once()
        self.assertIn('renamed', second['events'])
        # 旧 inode 的身份在改名后保持
        rotated = next(f for f in self.collector.positions() if f['inode'] == old_inode)
        self.assertEqual(Path(rotated['path']).name, 'sensor.1.csv')
        self.assertEqual(self.events(), {'e1', 'e2', 'e3'})

        # tail：旧文件迟到的跨行尾部 e4（数值含换行，业务拒收但不丢）
        self.append('sensor.1.csv', b'e4,sensor1,2026-11-11T10:00:04Z,"2\n3",C\n')
        self.collector.run_once()
        quarantined = review.listing(self.db, limit=10)
        self.assertEqual([q['reasons']['code'] for q in quarantined], ['invalid_number'])
        self.assertIn('2\n3', quarantined[0]['payload']['raw'])

        # replace：同名路径被新 inode 接管
        self.write('replacement', HEADER + b'e5,sensor1,2026-11-11T10:00:05Z,24,C\n')
        import os
        os.replace(self.logs / 'replacement', self.logs / 'sensor.csv')
        self.collector.run_once()
        self.assertIn('e5', self.events())
        self.assertTrue(any(f['state'] == 'replaced' for f in self.collector.positions()))

        # truncate：同 inode 截断重写，且新内容恰好等长，靠锚点识别新世代
        before = next(f for f in self.collector.positions()
                      if f['path'].endswith('sensor.csv') and f['state'] == 'following')
        self.write('sensor.csv', HEADER + b'e6,sensor1,2026-11-11T10:00:06Z,25,C\n')
        self.collector.run_once()
        after = [f for f in self.collector.positions() if f['path'].endswith('sensor.csv')]
        self.assertTrue(any(f['state'] == 'truncated' for f in after))
        self.assertTrue(any(f['generation'] == before['generation'] + 1 for f in after))
        self.assertEqual(self.events(), {'e1', 'e2', 'e3', 'e5', 'e6'})

    def test_full_rotation_drill(self):
        self.drill()
        self.assertTrue(inspection.check(self.db)['ok'])
        self.assertTrue(journal.verify(self.db)['ok'])

    def test_lineage_points_to_exact_byte_ranges(self):
        self.drill()
        rows = self.collector.lineage()
        # e1 与 e2、e4 同属旧 inode 的第一世代（改名后为 sensor.1.csv）。
        # 血缘 path 是行入库时的路径快照，因此用 file_key（inode+世代）定位。
        old_generation = next(
            f['file_key'] for f in self.collector.positions()
            if f['path'].endswith('sensor.1.csv')
        )
        target = next(
            row for row in rows
            if row['file_key'] == old_generation and row['raw_text'].startswith('e1,')
        )
        raw = (self.logs / 'sensor.1.csv').read_bytes()
        self.assertEqual(
            raw[target['start_offset']:target['end_offset']],
            b'e1,sensor1,2026-11-11T10:00:01Z,20,C\n',
        )
        # 每个文件世代内偏移连续、无重叠
        for file_key in {row['file_key'] for row in rows}:
            spans = sorted(
                (row['start_offset'], row['end_offset'])
                for row in rows if row['file_key'] == file_key
            )
            for (_, end), (start, _) in zip(spans, spans[1:]):
                self.assertEqual(end, start)
        # 血缘行与 raw_rows 一一对应
        self.assertEqual(
            self.db.scalar('SELECT COUNT(*) FROM collector_lineage'),
            self.db.scalar('SELECT COUNT(*) FROM raw_rows'),
        )


class CrashAtomicityTests(CollectorCase):
    def test_crash_after_import_before_checkpoint_leaves_no_observation(self):
        self.write('a.csv', HEADER +
                   b'e1,sensor1,2026-11-11T10:00:01Z,20,C\n'
                   b'e2,sensor1,2026-11-11T10:00:02Z,21,C\n')
        self.collector.failpoint_after_rows = True
        with self.assertRaises(RuntimeError):
            self.collector.run_once()
        # 业务读数与位置检查点同事务，崩溃后回滚：没有任何有效读数，
        # 文件检查点停在起始位置（半条/已读字节未被确认）。
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 0)
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM batches'), 0)
        self.assertEqual(
            self.db.scalar('SELECT COALESCE(MAX(position),0) FROM collector_files'), 0,
        )

        # 重启：重新打开数据库，从头跟随，不产生第二份读数
        self.db.close()
        db = Database(self.db_path)
        collector = Collector(db, 'feed1', self.logs, name='hangar')
        collector.run_once()
        self.assertEqual(db.scalar('SELECT COUNT(*) FROM observations'), 2)
        collector.run_once()  # 再扫一轮
        self.assertEqual(db.scalar('SELECT COUNT(*) FROM observations'), 2)
        self.assertTrue(inspection.check(db)['ok'])
        db.close()
        self.db = Database(self.db_path)  # 供 tearDown 关闭


class FairnessTests(CollectorCase):
    def test_small_file_is_served_in_first_round(self):
        big = HEADER + b''.join(
            f'e{i},sensor1,2026-11-11T10:{i // 60:02d}:{i % 60:02d}Z,{i % 50},C\n'.encode()
            for i in range(1, 200)
        )
        self.write('big.csv', big)
        self.write('small.csv', HEADER + b's1,sensor1,2026-11-11T09:00:00Z,10,C\n')
        collector = Collector(self.db, 'feed1', self.logs, name='fair', budget_bytes=600)
        summary = collector.run_once()
        self.assertEqual(summary['scanned'], 2)  # 两个文件同一轮都被轮询
        self.assertIn('s1', self.events())


class RestartContinuityTests(CollectorCase):
    def test_pending_half_record_survives_restart(self):
        self.write('sensor.csv', HEADER + b'e1,sensor1,2026-11-11T10:00:01Z,20,C\n'
                   b'e2,sensor1,2026-11-11T10:00:02Z,"2')
        self.collector.run_once()
        self.db.close()

        db = Database(self.db_path)
        collector = Collector(db, 'feed1', self.logs, name='hangar')
        # 重启后先空扫，状态仍认得半条
        self.assertEqual(collector.run_once()['records'], 0)
        self.append('sensor.csv', b'1",C\n')
        collector.run_once()
        self.assertEqual(
            db.all('SELECT external_event FROM observations ORDER BY external_event'),
            [{'external_event': 'e1'}, {'external_event': 'e2'}],
        )
        db.close()
        self.db = Database(self.db_path)


class BatchIdentityTests(CollectorCase):
    def test_batch_id_is_stable_for_generation(self):
        first = batch_id_for('feed1', 'inode-9-gen-1', 1)
        second = batch_id_for('feed1', 'inode-9-gen-1', 1)
        other_gen = batch_id_for('feed1', 'inode-9-gen-2', 2)
        self.assertEqual(first, second)
        self.assertNotEqual(first, other_gen)


if __name__ == '__main__':
    unittest.main()
