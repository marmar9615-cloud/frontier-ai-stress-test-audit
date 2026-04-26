import csv, math, tempfile, subprocess, sys, os
from pathlib import Path
ROOT=Path('/mnt/data/model_audit_work/extracted/claudecodetest(1)/claudecodetest/cctest6')
sys.path.insert(0, str(ROOT/'src'))
from analytics.aggregator import aggregate
from analytics.parser import iter_rows, open_csv

def write(lines):
    p=Path(tempfile.mkstemp(suffix='.csv')[1]); p.write_text('\n'.join(lines)+'\n', encoding='utf-8'); return p

def run(path, group):
    with open_csv(path) as f: return aggregate(iter_rows(f), group, None, None)

# quoted comma/unicode/invalid timestamp/huge amount
p=Path(tempfile.mkstemp(suffix='.csv')[1])
with open(p,'w',encoding='utf-8',newline='') as f:
    w=csv.writer(f); w.writerow(['timestamp','user_id','account_id','event_type','amount','country'])
    w.writerow(['not-a-date','u_bad','a','buy','10','US'])
    w.writerow(['2024-01-01T00:00:00','u1','a','buy','10','Sao Tome, Principe'])
    w.writerow(['2024-01-01T00:00:00','u2','a','buy','1e308','中国'])
r=run(p,['country'])
assert r.warnings==1, r
assert ('Sao Tome, Principe',) in r.groups
assert ('中国',) in r.groups
assert math.isfinite(r.groups[('中国',)]['sum']) and r.groups[('中国',)]['sum']>=1e308
# zero-byte CLI exits nonzero
z=Path(tempfile.mkstemp(suffix='.csv')[1]); z.write_text('',encoding='utf-8')
proc=subprocess.run(['/usr/bin/python3','-m','analytics',str(z),'--group-by','country'],cwd=ROOT,env={'PYTHONPATH': str(ROOT/'src'), 'PYTHONHASHSEED':'0'},capture_output=True,text=True,timeout=5)
assert proc.returncode==1 and ('empty' in proc.stderr.lower() or 'header' in proc.stderr.lower()), (proc.returncode, proc.stderr)
print('manual cctest6 checks passed')
