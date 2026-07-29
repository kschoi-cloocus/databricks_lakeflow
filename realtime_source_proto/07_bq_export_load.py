#!/usr/bin/env python3
"""Part A 경로2 — BigQuery 배치 export → UC Volume 랜딩 → COPY INTO Delta (일회성 마이그레이션).
   SA 키만으로 완전 제어 가능(dataViewer+jobUser). 실제 대규모 이관의 표준 패턴."""
import json, subprocess, time, os
from datetime import datetime, date
from google.cloud import bigquery
from google.oauth2 import service_account

P="DEFAULT"; WID="e6d81062ddc9efdc"; CAT="adb_wrkspc_krc_dev"; SCH="iot_proto"
KEY="/mnt/d/googledrive.ai/Work/AI/claude-code-beamrock/.secrets/google-sa.json"
BQ="myproject-493803.iot_proto"
WORK=os.path.dirname(os.path.abspath(__file__))
VOL=f"/Volumes/{CAT}/{SCH}/landing"

def cli(a, inp=None): return subprocess.run(["databricks"]+a+["-p",P], input=inp, capture_output=True, text=True)
def sql(stmt):
    body={"warehouse_id":WID,"catalog":CAT,"schema":SCH,"statement":stmt,"wait_timeout":"50s"}
    d=json.loads(cli(["api","post","/api/2.0/sql/statements","--json",json.dumps(body)]).stdout)
    sid=d.get("statement_id")
    for _ in range(25):
        if (d.get("status") or {}).get("state") in ("SUCCEEDED","FAILED","CANCELED"): break
        time.sleep(3); d=json.loads(cli(["api","get",f"/api/2.0/sql/statements/{sid}"]).stdout)
    return d
def rows(d): return (d.get("result") or {}).get("data_array") or []
def state(d): return (d.get("status") or {}).get("state"), ((d.get("status") or {}).get("error") or {}).get("message","")

def jdefault(o):
    if isinstance(o,(datetime,date)): return o.isoformat()
    return str(o)

# 1) BigQuery → 로컬 NDJSON (배치 추출)
cred=service_account.Credentials.from_service_account_file(KEY)
bq=bigquery.Client(project="myproject-493803", credentials=cred, location="asia-northeast3")
exports={
  "telemetry": (f"SELECT device_id,event_ts,metric,value,unit,battery_pct,ingest_date FROM `{BQ}.device_telemetry_history`","bq_telemetry.json"),
  "catalog":   (f"SELECT device_id,model,region,install_date FROM `{BQ}.device_catalog`","bq_catalog.json"),
}
for k,(q,fn) in exports.items():
    n=0
    with open(os.path.join(WORK,fn),"w") as f:
        for r in bq.query(q).result():
            f.write(json.dumps(dict(r), default=jdefault)+"\n"); n+=1
    print(f"1) export {k}: {n} rows → {fn}")

# 2) UC Volume 생성 + 업로드
print("2) UC Volume 랜딩 + 업로드")
print("   volume:", state(sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.{SCH}.landing"))[0])
for fn in ("bq_telemetry.json","bq_catalog.json"):
    r=cli(["fs","cp",os.path.join(WORK,fn),f"dbfs:{VOL}/{fn}","--overwrite"])
    print(f"   upload {fn}:", "ok" if r.returncode==0 else r.stderr[:120])

# 3) COPY INTO(읽기) → Delta (타입 캐스팅)
print("3) read_files → Delta CTAS")
tele_ctas=f"""CREATE OR REPLACE TABLE {CAT}.{SCH}.bq_telemetry_bronze AS
SELECT device_id, CAST(event_ts AS TIMESTAMP) event_ts, metric, CAST(value AS DOUBLE) value,
       unit, CAST(battery_pct AS INT) battery_pct, CAST(ingest_date AS DATE) ingest_date
FROM read_files('{VOL}/bq_telemetry.json', format=>'json')"""
cat_ctas=f"""CREATE OR REPLACE TABLE {CAT}.{SCH}.bq_device_catalog AS
SELECT device_id, model, region, CAST(install_date AS DATE) install_date
FROM read_files('{VOL}/bq_catalog.json', format=>'json')"""
print("   bq_telemetry_bronze:", state(sql(tele_ctas)))
print("   bq_device_catalog  :", state(sql(cat_ctas)))
sql(f"ALTER TABLE {CAT}.{SCH}.bq_telemetry_bronze SET TBLPROPERTIES ('source'='bigquery.batch_export','quality'='bronze')")

# 4) 검증 + BigQuery 대조
print("4) 검증 (BigQuery ↔ Delta 대조)")
print("   Delta bronze rows:", rows(sql(f"SELECT count(*) FROM {CAT}.{SCH}.bq_telemetry_bronze")))
print("   Delta catalog rows:", rows(sql(f"SELECT count(*) FROM {CAT}.{SCH}.bq_device_catalog")))
d=sql(f"""SELECT c.region, count(*) readings, round(avg(t.value),2) avg_val, count(DISTINCT t.device_id) devices
          FROM {CAT}.{SCH}.bq_telemetry_bronze t JOIN {CAT}.{SCH}.bq_device_catalog c USING(device_id)
          GROUP BY c.region ORDER BY readings DESC""")
print("   region 집계(Delta 조인):")
for r in rows(d): print("    ",r)
print("DONE")
