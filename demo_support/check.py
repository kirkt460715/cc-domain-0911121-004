import csv,io,tempfile
from pathlib import Path
from rotate import step
with tempfile.TemporaryDirectory() as d:
    step(d,'start');p=Path(d)/'sensor.csv';old=p.stat().st_ino;assert p.read_bytes().endswith(b'"2')
    step(d,'rotate');assert (Path(d)/'sensor.1.csv').stat().st_ino==old
    step(d,'tail');rows=list(csv.DictReader(io.StringIO((Path(d)/'sensor.1.csv').read_text(),newline='')));assert rows[-1]['value']=='2\n3'
    step(d,'replace');assert 'e5' in p.read_text();step(d,'truncate');assert 'e6' in p.read_text()
print('追加、改名、旧尾、替换及截断演练检查通过')
