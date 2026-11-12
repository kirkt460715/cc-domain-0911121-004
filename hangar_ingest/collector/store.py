'''采集检查点与字节血缘的持久化访问。

文件状态以 (collector, file_key) 为主键，file_key 同时携带 inode 与世代，
因此同一路径被替换或截断后会留下新的世代行，旧世代行保留可追溯。
'''

from hangar_ingest.types import fail, identity, integer, text


COLUMNS = (
    'collector,file_key,path,inode,generation,state,position,read_position,pending,in_quotes,'
    'line_number,header_ok,mtime,anchor_hash,anchor_len,first_seen,updated_at'
)


def list_files(db, collector):
    identity(collector, 'collector')
    rows = db.all(
        f'SELECT {COLUMNS} FROM collector_files WHERE collector=? ORDER BY path,generation',
        (collector,),
    )
    for row in rows:
        row['in_quotes'] = bool(row['in_quotes'])
        row['header_ok'] = bool(row['header_ok'])
    return rows


def get_file(db, collector, file_key):
    row = db.one(
        f'SELECT {COLUMNS} FROM collector_files WHERE collector=? AND file_key=?',
        (collector, file_key),
    )
    if row is not None:
        row['in_quotes'] = bool(row['in_quotes'])
        row['header_ok'] = bool(row['header_ok'])
    return row


def upsert_file(db, entry, *, now):
    required = {'collector', 'file_key', 'path', 'inode', 'generation', 'state',
                'position', 'read_position', 'pending', 'in_quotes', 'line_number',
                'header_ok', 'mtime', 'anchor_hash', 'anchor_len'}
    missing = required - set(entry)
    if missing:
        fail('collector_state', 'Collector file state missing fields', missing=sorted(missing))
    identity(entry['collector'], 'collector')
    identity(entry['file_key'], 'file_key')
    text(entry['path'], 'path')
    integer(entry['inode'], 'inode', 1)
    integer(entry['generation'], 'generation', 1)
    text(entry['state'], 'state')
    integer(entry['position'], 'position')
    integer(entry['read_position'], 'read_position')
    integer(entry['anchor_len'], 'anchor_len')
    if not isinstance(entry['mtime'], (int, float)):
        fail('collector_state', 'mtime must be numeric')
    if not isinstance(entry['anchor_hash'], str):
        fail('collector_state', 'anchor_hash must be text')
    if not isinstance(entry['pending'], (bytes, bytearray)):
        fail('collector_state', 'Pending tail must be bytes')
    existing = get_file(db, entry['collector'], entry['file_key'])
    values = (
        entry['path'], entry['inode'], entry['generation'], entry['state'],
        entry['position'], entry['read_position'], bytes(entry['pending']),
        int(bool(entry['in_quotes'])), entry['line_number'],
        int(bool(entry['header_ok'])), float(entry['mtime']),
        entry['anchor_hash'], entry['anchor_len'],
    )
    if existing is None:
        db.execute(
            f'INSERT INTO collector_files({COLUMNS}) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (entry['collector'], entry['file_key'], *values, now, now),
        )
    else:
        db.execute(
            'UPDATE collector_files SET path=?,inode=?,generation=?,state=?,position=?,'
            'read_position=?,pending=?,in_quotes=?,line_number=?,header_ok=?,mtime=?,'
            'anchor_hash=?,anchor_len=?,updated_at=? '
            'WHERE collector=? AND file_key=?',
            (*values, now, entry['collector'], entry['file_key']),
        )
    return get_file(db, entry['collector'], entry['file_key'])


def add_lineage(db, *, batch_id, row_number, collector, file_key, generation, path,
                start_offset, end_offset, raw_text, now):
    identity(batch_id, 'batch_id')
    integer(row_number, 'row_number', 1)
    db.execute(
        'INSERT INTO collector_lineage(batch_id,row_number,collector,file_key,generation,path,'
        'start_offset,end_offset,raw_text,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
        (batch_id, row_number, collector, file_key, generation, path,
         start_offset, end_offset, raw_text, now),
    )


def lineage_for_batch(db, batch_id):
    return db.all(
        'SELECT * FROM collector_lineage WHERE batch_id=? ORDER BY row_number', (batch_id,),
    )


def lineage_report(db, collector):
    rows = db.all(
        'SELECT l.*,r.state AS row_state FROM collector_lineage l '
        'JOIN raw_rows r ON r.batch_id=l.batch_id AND r.row_number=l.row_number '
        'WHERE l.collector=? ORDER BY l.file_key,l.start_offset',
        (collector,),
    )
    return rows
