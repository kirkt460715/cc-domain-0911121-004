'''采集主流水线。

完整记录复用既有传感器解码（normalize）与 processor 的校准、质量规则、
批次入库流程；采集侧额外保证：

- 检查点（文件位置、EOF 残余、世代状态）与业务写入在同一个数据库事务内提交，
  因此「导入成功后检查点未保存」在崩溃后整体回滚，重启不会产生第二份有效读数；
- 每条入库/拒收记录都带有来源文件世代与原始字节范围（collector_lineage）；
- 每轮扫描按文件轮询并分配字节预算，高频文件不能长期挤占其他文件。
'''

import hashlib

from hangar_ingest.types import PipelineError, canonical, digest, fail, identity
from hangar_ingest.decoders.sensors import normalize
from hangar_ingest.runs import journal, processor
from hangar_ingest.collector import store
from hangar_ingest.collector.framer import CsvFramer, is_header, parse_frame
from hangar_ingest.collector.watcher import DEFAULT_BUDGET_BYTES, DirectoryWatcher


def batch_id_for(source, file_key, generation):
    identity(source, 'source')
    return 'collect-' + hashlib.sha256(canonical([source, file_key, generation]).encode()).hexdigest()[:24]


def stage_collector_rows(db, *, batch_id, source, rows, collector, tracked, now):
    '''按文件世代幂等追加批次行，同时写入字节血缘。

    rows: [(row_number, payload_dict|None, error_dict|None, raw_text,
            start_offset, end_offset)]
    '''
    identity(batch_id, 'batch_id')
    with db.transaction():
        batch = db.one('SELECT * FROM batches WHERE batch_id=?', (batch_id,))
        if batch is None:
            placeholder = hashlib.sha256(
                canonical([source, tracked.file_key, tracked.generation]).encode(),
            ).hexdigest()
            db.execute(
                "INSERT INTO batches(batch_id,source,content_hash,format,state,revision,cursor,"
                "total,accepted,rejected,created_at,updated_at) "
                "VALUES(?,?,?,'csv','staged',1,0,0,0,0,?,?)",
                (batch_id, source, placeholder, now, now),
            )
            journal.record(db, 'collector.batch_opened', {
                'batch_id': batch_id, 'source': source,
                'file_key': tracked.file_key, 'generation': tracked.generation,
            })
        inserted = 0
        for row_number, payload, error, raw_text, start_offset, end_offset in rows:
            existing = db.one(
                'SELECT 1 FROM raw_rows WHERE batch_id=? AND row_number=?',
                (batch_id, row_number),
            )
            if existing:
                continue
            stored_payload = payload if payload is not None else {'raw': raw_text}
            db.execute(
                'INSERT INTO raw_rows(batch_id,row_number,payload,raw_hash,state,error) '
                "VALUES(?,?,?,?,?,?)",
                (batch_id, row_number, canonical(stored_payload), digest(stored_payload),
                 'invalid' if error else 'pending', canonical(error) if error else None),
            )
            store.add_lineage(
                db, batch_id=batch_id, row_number=row_number, collector=collector,
                file_key=tracked.file_key, generation=tracked.generation, path=tracked.path,
                start_offset=start_offset, end_offset=end_offset, raw_text=raw_text, now=now,
            )
            inserted += 1
        if inserted:
            total = db.scalar('SELECT COUNT(*) FROM raw_rows WHERE batch_id=?', (batch_id,))
            db.execute('UPDATE batches SET total=?,updated_at=? WHERE batch_id=?', (total, now, batch_id))
        return {'batch_id': batch_id, 'inserted': inserted}


class Collector:
    def __init__(self, db, source, directory, *, name='default', budget_bytes=DEFAULT_BUDGET_BYTES):
        self.db = db
        self.source = identity(source, 'source')
        self.name = identity(name, 'collector')
        self.directory = directory
        self.budget_bytes = budget_bytes
        # 测试钩子：在业务写入完成、检查点提交之前抛出，模拟崩溃。
        self.failpoint_after_rows = False

    def run_once(self, *, max_files=None, read_bytes=None):
        '''按预算只扫描一轮，返回本轮处理摘要。

        read_bytes 是「每个文件」的单轮读取配额，而非全局共享池：这样高频
        文件最多用掉自己的配额，下一个文件在同一轮仍会被轮询，不会被饿死。
        '''
        per_file_budget = read_bytes or self.budget_bytes
        watcher = DirectoryWatcher(self.db, self.name, self.directory)
        order, events = watcher.scan()
        # 扫描阶段的世代状态（改名、替换、消失、新世代）单独成事务落盘；
        # 它只描述文件身份，不含任何业务读数。数据行与位置检查点的原子性
        # 仍由各文件处理事务保证。
        with self.db.transaction():
            for tracked in watcher.tracked.values():
                self._persist(tracked)
            for event in events:
                journal.record(self.db, 'collector.file_event', {
                    'event': event[0], 'file_key': event[1].file_key, 'state': event[1].state,
                })
        if max_files is not None:
            order = order[:max_files]
        summary = {'scanned': 0, 'bytes_read': 0, 'records': 0, 'quarantined': 0,
                   'batches': set(), 'events': [event[0] for event in events]}
        for tracked in order:
            result = self._process_file(watcher, tracked, per_file_budget)
            summary['scanned'] += 1
            summary['bytes_read'] += result['bytes_read']
            summary['records'] += result['records']
            summary['quarantined'] += result['quarantined']
            summary['batches'].update(result['batches'])
        summary['batches'] = sorted(summary['batches'])
        return summary

    def follow(self, *, idle_polls=1, poll_interval=0.0):
        '''长期跟随：重复扫描，直到连续 idle_polls 轮没有新数据（测试/演示用）。'''
        import time
        idle = 0
        rounds = []
        while idle < idle_polls:
            result = self.run_once()
            rounds.append(result)
            if result['bytes_read'] == 0 and result['records'] == 0 and not result['events']:
                idle += 1
            else:
                idle = 0
            if poll_interval:
                time.sleep(poll_interval)
        return rounds

    def _process_file(self, watcher, tracked, budget):
        data, stat, kind, new_generation = watcher.read_slice(tracked, budget_bytes=budget)
        result = {'bytes_read': 0, 'records': 0, 'quarantined': 0, 'batches': []}
        if kind == 'stale':
            return result
        if kind == 'vanished':
            tracked.state = 'vanished'
            self._persist(tracked)
            journal.record(self.db, 'collector.file_event',
                           {'event': 'vanished', 'file_key': tracked.file_key, 'state': tracked.state})
            return result
        if kind in {'truncated', 'replaced'}:
            # 旧世代封存并落盘；截断/替换后的新世代在同一轮立即处理，避免漏扫。
            self._persist(tracked)
            journal.record(self.db, 'collector.generation_closed', {
                'file_key': tracked.file_key, 'state': tracked.state, 'position': tracked.position,
            })
            if new_generation is not None:
                nested = self._process_file(watcher, new_generation, budget)
                for key in ('bytes_read', 'records', 'quarantined'):
                    result[key] += nested[key]
                result['batches'].extend(nested['batches'])
            return result

        framer = CsvFramer()
        pending_prefix = tracked.pending
        if pending_prefix:
            framer.feed(pending_prefix)
        # 拼接区 = pending（旧残余）+ data（本轮新读），其磁盘绝对起点是
        # read_position - len(pending)，帧偏移据此换算为绝对字节范围。
        buffer_origin = tracked.read_position - len(pending_prefix)
        cursor = buffer_origin
        frames = framer.feed(data)
        result['bytes_read'] = len(data)
        # 磁盘读取高水位只随实际读取前进，与业务是否确认无关。
        tracked.read_position += len(data)

        staged_rows = []
        line_number = tracked.line_number
        for frame in frames:
            raw = frame['bytes']
            frame_start = cursor
            frame_end = cursor + len(raw)
            cursor = frame_end
            line_number += 1

            if not tracked.header_ok:
                if is_header(raw):
                    tracked.header_ok = True
                    continue
                tracked.state = 'invalid_header'
                self._persist(tracked)
                journal.record(self.db, 'collector.invalid_header', {
                    'file_key': tracked.file_key, 'path': tracked.path,
                })
                fail('csv_header', 'CSV header does not match sensor schema', path=tracked.path)

            payload, error = self._decode_frame(raw, frame['oversized'])
            staged_rows.append((
                line_number, payload, error, raw.decode('utf-8', errors='replace'),
                frame_start, frame_end,
            ))

        pending = framer.pending_bytes()
        # 业务确认位置只到「完整记录之后」；EOF 半条（含半个中文字符/未闭合
        # 引号）作为 pending，下次随新字节重新分帧，磁盘高水位不回退。
        # 恒有 position + len(pending) == read_position。
        tracked.position = tracked.read_position - len(pending)
        tracked.pending = pending
        tracked.in_quotes = framer.in_quotes
        tracked.line_number = line_number
        self._update_anchor(tracked)

        now = self.db.clock()
        if staged_rows:
            batch_id = batch_id_for(self.source, tracked.file_key, tracked.generation)
            result['batches'].append(batch_id)
            # 暂存、解码/校准/入库与检查点在同一事务内提交：崩溃则整体回滚，
            # 文件位置不会越过未确认的记录，重启不会产生第二份有效读数。
            with self.db.transaction():
                stage_collector_rows(
                    self.db, batch_id=batch_id, source=self.source, rows=staged_rows,
                    collector=self.name, tracked=tracked, now=now,
                )
                # 采集器会向同一文件世代的批次持续追加；上一轮若已到 complete/
                # partial，这里重新打开，让新行进入既有校准与质量流程。
                self.db.execute(
                    "UPDATE batches SET state='processing',updated_at=? "
                    "WHERE batch_id=? AND state IN ('complete','partial')",
                    (now, batch_id),
                )
                processed = processor.process(self.db, batch_id, limit=len(staged_rows))
                result['records'] += len(processed['processed'])
                result['quarantined'] += sum(
                    1 for item in processed['processed'] if item['state'] == 'quarantined'
                )
                if self.failpoint_after_rows:
                    self.failpoint_after_rows = False
                    raise RuntimeError('simulated crash before checkpoint commit')
                self._persist(tracked)
        else:
            self._persist(tracked)
        return result

    @staticmethod
    def _update_anchor(tracked, *, anchor_bytes=4096):
        '''记录文件前缀锚点哈希，用于识别同 inode 的截断重写。'''
        import hashlib
        from pathlib import Path
        path = Path(tracked.path)
        if not path.exists():
            return
        length = min(anchor_bytes, path.stat().st_size)
        if length == 0:
            return
        with path.open('rb') as handle:
            tracked.anchor_hash = hashlib.sha256(handle.read(length)).hexdigest()
        tracked.anchor_len = length

    @staticmethod
    def _decode_frame(raw, oversized):
        if oversized:
            return None, {'code': 'record_size', 'message': 'Sensor record exceeds one MiB'}
        try:
            fields = parse_frame(raw)
            if len(fields) != 5:
                return None, {'code': 'csv_columns', 'message': 'CSV row field count mismatch'}
            row = dict(zip(('event_id', 'device', 'observed_at', 'value', 'unit'), fields))
            return normalize(row), None
        except PipelineError as exc:
            return None, exc.as_dict()

    def _persist(self, tracked):
        store.upsert_file(self.db, tracked.as_row(), now=self.db.clock())

    def positions(self):
        return store.list_files(self.db, self.name)

    def lineage(self):
        return store.lineage_report(self.db, self.name)
