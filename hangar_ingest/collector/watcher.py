'''文件身份、世代追踪与公平扫描调度。

身份基于 inode 而非路径：
- 日志改名（sensor.csv -> sensor.1.csv）后 inode 不变，旧文件迟到的尾部
  仍然归到同一身份，进度不会丢；
- 同一路径被新 inode 替换时开启新的世代；
- 同一 inode 被截断重写时也开启新的世代，并记录 truncated 状态。

截断不能只靠文件大小判断：截断后重写的内容可能恰好与旧位置等长。
因此同时记录 mtime（浮点秒）——mtime 回退或跳变即视为同一 inode 上的新世代。

状态语义（state）：
- following：正在跟随的活跃文件；
- rotated：已改名离开原路径，但仍可能继续追加尾部；
- replaced：同一路径被别的文件替换，旧世代保留；
- truncated：同一文件被截断重写，旧世代保留；
- vanished：文件从目录中消失；
- invalid_header：文件头不符合 sensor CSV 约定，停止推进等待人工处理。
'''

from pathlib import Path

from hangar_ingest.types import fail, identity

# 公平调度：单轮内每个文件最多读取的字节数，避免高频日志长期挤占其他文件。
DEFAULT_BUDGET_BYTES = 1 * 1024 * 1024


def make_file_key(inode, generation):
    return f'inode-{inode}-gen-{generation}'


class TrackedFile:
    def __init__(self, row):
        self.collector = row['collector']
        self.file_key = row['file_key']
        self.path = row['path']
        self.inode = row['inode']
        self.generation = row['generation']
        self.state = row['state']
        self.position = row['position']
        self.read_position = row['read_position']
        self.pending = row['pending']
        self.in_quotes = row['in_quotes']
        self.line_number = row['line_number']
        self.header_ok = row['header_ok']
        self.mtime = row['mtime']
        self.anchor_hash = row['anchor_hash']
        self.anchor_len = row['anchor_len']

    def as_row(self):
        return {
            'collector': self.collector,
            'file_key': self.file_key,
            'path': self.path,
            'inode': self.inode,
            'generation': self.generation,
            'state': self.state,
            'position': self.position,
            'read_position': self.read_position,
            'pending': self.pending,
            'in_quotes': self.in_quotes,
            'line_number': self.line_number,
            'header_ok': self.header_ok,
            'mtime': self.mtime,
            'anchor_hash': self.anchor_hash,
            'anchor_len': self.anchor_len,
        }


INACTIVE_STATES = {'invalid_header', 'replaced', 'truncated', 'vanished'}


class DirectoryWatcher:
    def __init__(self, db, collector, directory, *, pattern='.csv'):
        self.db = db
        self.collector = identity(collector, 'collector')
        self.directory = Path(directory)
        self.pattern = pattern
        self.tracked = {}
        for row in self.db.all(
            'SELECT * FROM collector_files WHERE collector=? ORDER BY file_key', (self.collector,),
        ):
            row['in_quotes'] = bool(row['in_quotes'])
            row['header_ok'] = bool(row['header_ok'])
            tracked = TrackedFile(row)
            self.tracked[tracked.file_key] = tracked

    def _candidate_paths(self):
        if not self.directory.is_dir():
            fail('collector_directory', 'Watched directory does not exist', path=str(self.directory))
        paths = []
        for entry in sorted(self.directory.iterdir(), key=lambda item: item.name):
            if entry.is_symlink() or not entry.is_file():
                continue
            if self.pattern and entry.name.endswith(self.pattern):
                paths.append(entry)
        return paths

    def scan(self):
        '''把目录现状与追踪状态对齐，返回 (读取顺序, 事件列表)。'''
        events = []
        candidates = self._candidate_paths()
        current_inodes = {}
        current_paths = {}
        for entry in candidates:
            stat = entry.stat()
            current_inodes[stat.st_ino] = (entry, stat)
            current_paths[str(entry)] = stat.st_ino

        touched = set()
        # 1. 身份匹配：现存文件按 inode 找到自己的世代，改名在此发生。
        for inode, (entry, stat) in current_inodes.items():
            match = self._latest_for_inode(inode)
            if match is None:
                # 新 inode 出现：若占据了旧世代的路径，旧世代先标为轮转/替换。
                previous_owner = next(
                    (item for item in self.tracked.values()
                     if item.path == str(entry) and item.inode != inode),
                    None,
                )
                tracked = self._open_generation(entry, stat, state='following')
                if previous_owner is not None:
                    previous_owner.state = 'rotated' if previous_owner.inode in current_inodes else 'replaced'
                    events.append((previous_owner.state, previous_owner))
                events.append(('discovered', tracked))
            else:
                if match.path != str(entry):
                    events.append(('renamed', match, match.path, str(entry)))
                    match.path = str(entry)
                if match.state == 'vanished':
                    match.state = 'following'
                    events.append(('reappeared', match))
                touched.add(match.file_key)

        # 2. 目录里已不存在的 inode：路径被接管算替换，否则算消失。
        for tracked in list(self.tracked.values()):
            if tracked.file_key in touched or tracked.state in {'replaced', 'truncated', 'invalid_header'}:
                continue
            if tracked.inode not in current_inodes:
                owner_inode = current_paths.get(tracked.path)
                tracked.state = 'replaced' if owner_inode is not None else 'vanished'
                events.append((tracked.state, tracked))

        return self._read_order(), events

    def _latest_for_inode(self, inode):
        matches = [item for item in self.tracked.values() if item.inode == inode]
        return max(matches, key=lambda item: item.generation) if matches else None

    def _open_generation(self, entry, stat, *, state, generation=1):
        file_key = make_file_key(stat.st_ino, generation)
        while file_key in self.tracked:
            generation += 1
            file_key = make_file_key(stat.st_ino, generation)
        row = {
            'collector': self.collector,
            'file_key': file_key,
            'path': str(entry),
            'inode': stat.st_ino,
            'generation': generation,
            'state': state,
            'position': 0,
            'read_position': 0,
            'pending': b'',
            'in_quotes': False,
            'line_number': 0,
            'header_ok': False,
            'mtime': stat.st_mtime,
            'anchor_hash': '',
            'anchor_len': 0,
        }
        tracked = TrackedFile(row)
        self.tracked[file_key] = tracked
        return tracked

    def detect_change(self, tracked, stat):
        '''识别同一读取路径上的世代变化，返回 (kind, new_generation_or_None)。

        kind: unchanged / truncated / replaced / stale。
        - inode 变了：路径被新文件替换；
        - inode 相同但大小回退，或 mtime 跳变且前缀锚点内容变了：截断重写；
        - 其余视为追加。锚点哈希能识别等长或更长的重写，而不只是大小回退。
        '''
        latest = self._latest_for_inode(stat.st_ino)
        if latest is not None and latest.file_key != tracked.file_key:
            return 'stale', None
        replaced = stat.st_ino != tracked.inode
        truncated = not replaced and (
            stat.st_size < tracked.read_position
            or (
                stat.st_mtime != tracked.mtime
                and tracked.anchor_len
                and self._anchor_changed(tracked, stat)
            )
        )
        if not (replaced or truncated):
            return 'unchanged', None
        kind = 'replaced' if replaced else 'truncated'
        tracked.state = kind
        current = Path(tracked.path)
        new_generation = None
        if current.exists():
            new_generation = self._open_generation(current, current.stat(), state='following')
        return kind, new_generation

    @staticmethod
    def _anchor_changed(tracked, stat):
        import hashlib
        length = min(tracked.anchor_len, stat.st_size)
        if length == 0:
            return False
        with open(tracked.path, 'rb') as handle:
            digest = hashlib.sha256(handle.read(length)).hexdigest()
        return digest != tracked.anchor_hash

    def _read_order(self):
        # 轮询顺序按 file_key 固定排序；已封存/坏头文件不再读取，调用方逐文件
        # 分配预算即可保证高频文件不长期挤占其他文件。
        return [item for item in sorted(self.tracked.values(), key=lambda item: item.file_key)
                if item.state not in INACTIVE_STATES]

    def read_slice(self, tracked, *, budget_bytes):
        '''读取一个文件当前世代的下一段，返回 (data, stat, kind, new_generation)。

        读取起点是磁盘高水位 read_position；已分帧但未确认的 pending 字节
        留在内存状态里，不会被重复读取。
        '''
        path = Path(tracked.path)
        if not path.exists() or path.is_symlink():
            return b'', None, 'vanished', None
        stat = path.stat()
        kind, new_generation = self.detect_change(tracked, stat)
        if kind in {'truncated', 'replaced'}:
            return b'', stat, kind, new_generation
        if kind == 'stale':
            return b'', stat, 'stale', None
        if stat.st_size == tracked.read_position:
            return b'', stat, 'unchanged', None
        tracked.mtime = stat.st_mtime
        with open(path, 'rb') as handle:
            handle.seek(tracked.read_position)
            data = handle.read(budget_bytes)
        return data, stat, 'data', None
