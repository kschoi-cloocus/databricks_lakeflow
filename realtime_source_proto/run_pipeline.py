#!/usr/bin/env python3
"""SDP 노트북 업로드 → 파이프라인 생성 → 실행(full refresh) → 상태 폴링."""
import json, subprocess, time, sys, os

P="DEFAULT"
USER="kschoi@cloocus.com"
NB_LOCAL=os.path.join(os.path.dirname(os.path.abspath(__file__)), "04_sdp_eventhub_pipeline.py")
NB_WS=f"/Users/{USER}/iot_proto/sdp_eventhub_pipeline"
PIPE_NAME="iot_proto_eventhub_sdp"
CAT="adb_wrkspc_krc_dev"; SCH="iot_proto"

def cli(args, inp=None):
    return subprocess.run(["databricks"]+args+["-p",P], input=inp, capture_output=True, text=True)
def api(method, path, body=None):
    args=["api",method,path]
    if body is not None: args+=["--json",json.dumps(body)]
    r=cli(args)
    try: return json.loads(r.stdout) if r.stdout.strip() else {"_raw":r.stdout,"_err":r.stderr}
    except: return {"_raw":r.stdout,"_err":r.stderr}

# 1) 노트북 업로드
cli(["workspace","mkdirs",f"/Users/{USER}/iot_proto"])
r=cli(["workspace","import",NB_WS,"--file",NB_LOCAL,"--language","PYTHON","--format","SOURCE","--overwrite"])
print("1) notebook import:", "ok" if r.returncode==0 else r.stderr[:200])

# 2) 기존 파이프라인 조회(중복 방지)
pipes=api("get","/api/2.0/pipelines")
pid=None
for pl in (pipes.get("statuses") or []):
    if pl.get("name")==PIPE_NAME: pid=pl.get("pipeline_id"); break

spec={
    "name":PIPE_NAME,
    "serverless":True,
    "catalog":CAT,
    "schema":SCH,
    "development":True,
    "continuous":False,
    "channel":"CURRENT",
    "libraries":[{"notebook":{"path":NB_WS}}],
}
if pid:
    spec["id"]=pid
    r=cli(["api","put",f"/api/2.0/pipelines/{pid}","--json",json.dumps(spec)])
    print(f"2) pipeline update(existing): {pid}", "ok" if r.returncode==0 else r.stderr[:200])
else:
    d=api("post","/api/2.0/pipelines",spec)
    pid=d.get("pipeline_id")
    print("2) pipeline create:", pid or d)
if not pid: sys.exit("파이프라인 ID 없음")
open("/tmp/claude-1000/-home-beamrock-koiia-onboarding/14449e35-fd2d-4cff-99f7-3b3cbccb2004/scratchpad/azure/pipeline_id.txt","w").write(pid)

# 3) 실행(full refresh)
d=api("post",f"/api/2.0/pipelines/{pid}/updates",{"full_refresh":True})
upd=d.get("update_id")
print("3) update 시작:", upd or d)

# 4) 폴링
print("4) 진행 상태:")
last=None
for i in range(60):   # 최대 ~10분
    st=api("get",f"/api/2.0/pipelines/{pid}")
    state=st.get("state")
    latest=(st.get("latest_updates") or [{}])[0]
    us=latest.get("state")
    if (state,us)!=last:
        print(f"   [{i:02d}] pipeline={state} update={us}")
        last=(state,us)
    if us in ("COMPLETED","FAILED","CANCELED"): break
    if state=="FAILED": break
    time.sleep(10)

print("\n최종:", json.dumps({"pipeline_state":st.get("state"),
      "update_state":(st.get("latest_updates") or [{}])[0].get("state")}, ensure_ascii=False))
print("pipeline_id:", pid)
