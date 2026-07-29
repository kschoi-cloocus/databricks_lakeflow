#!/usr/bin/env python3
"""Cosmos device_state 스냅샷 → Delta 참조 테이블(브리지).
   서버리스 SDP는 커스텀 커넥터 JAR 제약 → Cosmos hot-state를 주기 스냅샷으로 Delta 반영.
   (운영 대안: 클래식 컴퓨트 + Cosmos Spark 커넥터 Change Feed 스트리밍)"""
import json, subprocess, time
from azure.cosmos import CosmosClient

P="DEFAULT"; WID="e6d81062ddc9efdc"; CAT="adb_wrkspc_krc_dev"; SCH="iot_proto"

# 1) Cosmos에서 device_state 전량 읽기
cfg=json.load(open("/mnt/d/googledrive.ai/Work/AI/claude-code-beamrock/.secrets/cosmos.json"))
cont=CosmosClient(cfg["endpoint"],cfg["key"]).get_database_client(cfg["database"]).get_container_client(cfg["container"])
docs=list(cont.query_items("SELECT c.device_id,c.model,c.firmware,c.region,c.site,c.status,c.registered_at,c.last_seen,c.battery_pct FROM c",
                           enable_cross_partition_query=True))
docs.sort(key=lambda d:d["device_id"])
print(f"Cosmos read: {len(docs)} device_state docs")

# 2) 리터럴 VALUES CTAS 생성
def s(v): return "NULL" if v is None else "'"+str(v).replace("'","''")+"'"
rows=[]
for d in docs:
    rows.append(f"({s(d['device_id'])},{s(d['model'])},{s(d['firmware'])},{s(d['region'])},{s(d['site'])},"
                f"{s(d['status'])},{s(d['registered_at'])},{s(d['last_seen'])},{d['battery_pct']})")
cols="device_id,model,firmware,region,site,status,registered_at,last_seen,battery_pct"
ctas=(f"CREATE OR REPLACE TABLE {CAT}.{SCH}.device_reference AS\n"
      f"SELECT * FROM (VALUES\n"+",\n".join(rows)+f"\n) AS t({cols})")

def sql(stmt):
    body={"warehouse_id":WID,"catalog":CAT,"schema":SCH,"statement":stmt,"wait_timeout":"50s"}
    p=subprocess.run(["databricks","api","post","/api/2.0/sql/statements","--json",json.dumps(body),"-p",P],capture_output=True,text=True)
    d=json.loads(p.stdout); sid=d.get("statement_id")
    for _ in range(20):
        st=(d.get("status") or {}).get("state")
        if st in ("SUCCEEDED","FAILED","CANCELED"): break
        time.sleep(3)
        d=json.loads(subprocess.run(["databricks","api","get",f"/api/2.0/sql/statements/{sid}","-p",P],capture_output=True,text=True).stdout)
    return d
def rows_of(d): return (d.get("result") or {}).get("data_array") or []

d=sql(ctas)
print("CTAS device_reference:", (d.get("status") or {}).get("state"), (d.get("status") or {}).get("error",""))
sql("ALTER TABLE device_reference SET TBLPROPERTIES ('quality'='reference','source'='cosmosdb.device_state')")
d=sql("SELECT count(*) c, count(DISTINCT region) regions FROM device_reference")
print("verify:", rows_of(d))
d=sql("SELECT device_id,model,region,status FROM device_reference ORDER BY device_id LIMIT 3")
for r in rows_of(d): print("  ",r)
