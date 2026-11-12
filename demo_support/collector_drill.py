'''目录采集器端到端演练。

用既有的 rotate.py 在独立临时目录里驱动完整文件生命期：
半条写入 → 轮转改名 → 旧文件迟到尾部 → 同名替换 → 截断重写，
并在中途模拟「导入成功、检查点未保存」的崩溃与重启。
最后打印业务批次、拒收原文与各文件世代的处理位置和字节血缘。

只读取本地合成数据，不访问任何外部服务。
'''

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rotate import step

from hangar_ingest.runs.database import Database
from hangar_ingest.quality import devices, rules, review
from hangar_ingest.runs import batches, inspection
from hangar_ingest.collector.pipeline import Collector


def prepare_database(path):
    db = Database(path)
    devices.register(db, 'dev1', 'plane1', 'engine1', 'temperature', 'C')
    devices.add_alias(
        db, 'feed1', 'sensor1', 'dev1',
        '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
    )
    devices.add_calibration(
        db, 'cal1', 'dev1',
        '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
    )
    rules.configure(db, 'dev1', minimum='-100', maximum='200')
    return db


def print_positions(collector):
    print('\n=== 各文件世代处理位置 ===')
    for entry in collector.positions():
        name = Path(entry['path']).name
        print(
            f"  {entry['file_key']} 状态={entry['state']:<9} "
            f"文件={name:<12} 确认位置={entry['position']:<4} "
            f"读取高水位={entry['read_position']:<4} "
            f"待处理半条={len(entry['pending'])} 字节",
        )


def print_batches(db):
    print('\n=== 业务批次 ===')
    for batch in batches.listing(db):
        print(
            f"  {batch['batch_id']} 状态={batch['state']:<9} "
            f"总行={batch['total']} 接受={batch['accepted']} 拒收={batch['rejected']} "
            f"游标={batch['cursor']}",
        )


def print_rejections(db):
    print('\n=== 拒收原文（开放隔离区）===')
    entries = review.listing(db, status='open', limit=100)
    if not entries:
        print('  （无）')
    for entry in entries:
        print(f"  批次 {entry['batch_id'][-8:]} 行 {entry['row_number']} "
              f"原因={entry['reasons']['code']}")
        print(f"    原文: {entry['payload'].get('raw', '')!r}")


def print_lineage(collector):
    print('\n=== 字节血缘（结果 → 原始字节范围）===')
    for row in collector.lineage():
        print(
            f"  {row['file_key']} 行{row['row_number']} "
            f"字节[{row['start_offset']},{row['end_offset']}) "
            f"状态={row['row_state']:<11} {row['raw_text'].splitlines()[0][:38]!r}",
        )


def main():
    with tempfile.TemporaryDirectory() as workspace:
        logs = Path(workspace) / 'logs'
        db_path = Path(workspace) / 'hangar.db'

        # 阶段 1：半条写入
        step(logs, 'start')
        db = prepare_database(db_path)
        collector = Collector(db, 'feed1', logs, name='hangar')
        collector.run_once()
        print('阶段 start：半条 e2 停在 EOF，不产生假记录')

        # 阶段 2：轮转改名 + 新文件
        step(logs, 'rotate')
        collector.run_once()
        print('阶段 rotate：旧 inode 改名后继续跟随，半条 e2 补全入库')

        # 崩溃演练：业务写入后、检查点提交前崩溃
        collector.failpoint_after_rows = True
        step(logs, 'tail')
        try:
            collector.run_once()
        except RuntimeError:
            print('\n阶段 tail（崩溃演练）：导入后检查点提交前崩溃，事务整体回滚')
        observations_before = db.scalar('SELECT COUNT(*) FROM observations')
        db.close()

        # 重启：重新打开数据库继续跟随，验证不产生第二份有效读数
        db = Database(db_path)
        collector = Collector(db, 'feed1', logs, name='hangar')
        collector.run_once()  # 迟到尾部在重启后被处理
        observations_after = db.scalar('SELECT COUNT(*) FROM observations')
        print(f'重启后观察数 {observations_before} → {observations_after}（无重复读数）')

        # 阶段 4、5：同名替换与截断重写
        step(logs, 'replace')
        collector.run_once()
        step(logs, 'truncate')
        collector.run_once()
        print('阶段 replace/truncate：旧世代封存，新世代独立跟随')

        print_batches(db)
        print_rejections(db)
        print_positions(collector)
        print_lineage(collector)

        events = [row['external_event']
                  for row in db.all('SELECT external_event FROM observations ORDER BY external_event')]
        print('\n=== 入库有效读数 ===')
        print(' ', events)
        check = inspection.check(db)
        print('\n完整性检查:', '通过' if check['ok'] else check['errors'])
        assert check['ok']
        assert events == ['e1', 'e2', 'e3', 'e5', 'e6']
        db.close()
        print('\n端到端演练通过')


if __name__ == '__main__':
    main()
