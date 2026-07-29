#!/usr/bin/env python3
"""DQX 노트북 업로드 → serverless one-time job 실행 → 결과 폴링."""
import json, subprocess, time, os, sys
P="DEFAULT"; USER="kschoi@cloocus.com"
NB_LOCAL=os.path.join(os.path.dirname(os.path.abspath(__file__)), "08_dqx_quality.py")
NB_WS=f"/Users/{USER}/iot_proto/08_dqx_quality"

def cli(a, inp=None): return subprocess.run(["databricks"]+a+["-p",P], input=inp, capture_output=True, text=True)
def api(method, path, body=None):
    a=["api",method,path]+(["--json",json.dumps(body)] if body is not None else [])
    r=cli(a)
    try: return json.loads(r.stdout) if r.stdout.strip() else {"_err":r.stderr}
    except: return {"_raw":r.stdout[:400],"_err":r.stderr[:400]}

# 1) 업로드
cli(["workspace","mkdirs",f"/Users/{USER}/iot_proto"])
r=cli(["workspace","import",NB_WS,"--file",NB_LOCAL,"--language","PYTHON","--format","SOURCE","--overwrite"])
print("1) import:", "ok" if r.returncode==0 else r.stderr[:200])

# 2) serverless one-time run 제출
body={"run_name":"iot_proto_dqx",
      "tasks":[{"task_key":"dqx","notebook_task":{"notebook_path":NB_WS},"environment_key":"dqxenv"}],
      "environments":[{"environment_key":"dqxenv","spec":{"client":"2","dependencies":["databricks-labs-dqx"]}}]}
d=api("post","/api/2.1/jobs/runs/submit",body)
run_id=d.get("run_id")
print("2) submit run_id:", run_id or d)
if not run_id: sys.exit(1)

# 3) 폴링
last=None
for i in range(80):  # 최대 ~13분
    g=api("get",f"/api/2.1/jobs/runs/get?run_id={run_id}")
    st=(g.get("state") or {})
    life=st.get("life_cycle_state"); res=st.get("result_state")
    if (life,res)!=last:
        print(f"   [{i:02d}] {life} {res or ''} {st.get('state_message','')[:80]}")
        last=(life,res)
    if life in ("TERMINATED","SKIPPED","INTERNAL_ERROR"): break
    time.sleep(10)

# 4) 결과
o=api("get",f"/api/2.1/jobs/runs/get-output?run_id={run_id}")
no=(o.get("notebook_output") or {})
print("\n최종 result_state:", (g.get('state') or {}).get('result_state'))
print("notebook_output:", no.get("result") or "(exit 없음)")
if (g.get('state') or {}).get('result_state')!="SUCCESS":
    print("ERROR trace:", (o.get("error") or o.get("error_trace") or "")[:1200])
