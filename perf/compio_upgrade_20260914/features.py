import os,subprocess,json
from pathlib import Path
root=Path(__file__).resolve().parent
binary=next(p for p in (root/'upgrade/target/release/deps').glob('compio_features-*') if os.access(p,os.X_OK) and p.is_file())
results=[]
for repeat in range(3):
 for mode,scheduler in [('ordinary','default'),('managed','default'),('multi','default'),('poll-first','default'),('ordinary','single'),('ordinary','defer'),('ordinary','sqpoll')]:
  for size in [4096,65536,1048576,8388608]:
   try:
    r=subprocess.run([str(binary),mode,scheduler,str(size)],capture_output=True,text=True,timeout=30)
    result=dict(mode=mode,scheduler=scheduler,size=size,repeat=repeat,returncode=r.returncode,stdout=r.stdout,stderr=r.stderr)
   except subprocess.TimeoutExpired as e:result=dict(mode=mode,scheduler=scheduler,size=size,repeat=repeat,returncode='timeout')
   results.append(result)
   print(mode,scheduler,size,repeat,result['returncode'],result.get('stdout','').strip(),flush=True)
   (root/'results'/'features.json').write_text(json.dumps(results,indent=2))
