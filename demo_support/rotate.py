import argparse,os
from pathlib import Path
HEADER=b'event_id,device,observed_at,value,unit\n'
FIRST=b'e1,sensor1,2026-11-11T10:00:01Z,20,C\n'
def step(root,stage):
    root=Path(root);root.mkdir(parents=True,exist_ok=True);p=root/'sensor.csv'
    if stage=='start':p.write_bytes(HEADER+FIRST+b'e2,sensor1,2026-11-11T10:00:02Z,"2')
    elif stage=='rotate':
        with p.open('ab') as f:f.write(b'1",C\n')
        p.rename(root/'sensor.1.csv');p.write_bytes(HEADER+b'e3,sensor1,2026-11-11T10:00:03Z,22,C\n')
    elif stage=='tail':
        with (root/'sensor.1.csv').open('ab') as f:f.write(b'e4,sensor1,2026-11-11T10:00:04Z,"2\n3",C\n')
    elif stage=='replace':
        temp=root/'replacement';temp.write_bytes(HEADER+b'e5,sensor1,2026-11-11T10:00:05Z,24,C\n');os.replace(temp,p)
    elif stage=='truncate':p.write_bytes(HEADER+b'e6,sensor1,2026-11-11T10:00:06Z,25,C\n')
    else:raise ValueError('未知演练阶段')
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root');p.add_argument('stage',choices=['start','rotate','tail','replace','truncate']);a=p.parse_args();step(a.root,a.stage);print('日志阶段已应用',a.stage)
