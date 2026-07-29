#!/usr/bin/env python3
"""BigQuery 중립 IoT 이력 데이터셋 생성 — '기존 warehouse' 역할 (Part A 원천).
   Part B(Event Hub 실시간)와 동일한 단말기 모델을 공유해 하나의 IoT 서사로 연결."""
import random
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

KEY="/mnt/d/googledrive.ai/Work/AI/claude-code-beamrock/.secrets/google-sa.json"
PROJECT="myproject-493803"; DS="iot_proto"; LOC="asia-northeast3"
cred=service_account.Credentials.from_service_account_file(KEY)
bq=bigquery.Client(project=PROJECT, credentials=cred, location=LOC)

# 1) 데이터셋
ds=bigquery.Dataset(f"{PROJECT}.{DS}"); ds.location=LOC
ds.description="IoT 단말기 이력 warehouse (중립 샘플, Databricks 이관 대상)"
bq.create_dataset(ds, exists_ok=True)
print(f"dataset ready: {PROJECT}.{DS} @ {LOC}")

MODELS=["EnvSense-X2","EnvSense-X3","AirNode-Pro"]
REGIONS=["KR-Seoul","KR-Busan","JP-Tokyo","US-East"]
random.seed(11)
devices=[f"SN-{i:04d}" for i in range(1,21)]
dev_meta={d:(random.choice(MODELS),random.choice(REGIONS),
             (datetime.now(timezone.utc)-timedelta(days=random.randint(60,500))).date().isoformat())
          for d in devices}

# 2) device_catalog (차원)
cat_schema=[bigquery.SchemaField(n,t) for n,t in
    [("device_id","STRING"),("model","STRING"),("region","STRING"),("install_date","DATE")]]
cat_tbl=bigquery.Table(f"{PROJECT}.{DS}.device_catalog", schema=cat_schema)
cat_tbl.description="단말기 카탈로그(차원)"
bq.create_table(cat_tbl, exists_ok=True)
cat_rows=[{"device_id":d,"model":m[0],"region":m[1],"install_date":m[2]} for d,m in dev_meta.items()]
bq.load_table_from_json(cat_rows, f"{PROJECT}.{DS}.device_catalog",
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE",schema=cat_schema)).result()
print(f"device_catalog loaded: {len(cat_rows)} rows")

# 3) device_telemetry_history (사실 — 30일 이력)
METRICS={"temperature":( -10,60,"C"),"humidity":(0,100,"pct"),
         "co2":(400,1800,"ppm"),"vibration":(0,45,"mm_s")}
fact_schema=[bigquery.SchemaField(n,t) for n,t in
    [("device_id","STRING"),("event_ts","TIMESTAMP"),("metric","STRING"),
     ("value","FLOAT"),("unit","STRING"),("battery_pct","INTEGER"),("ingest_date","DATE")]]
fact_tbl=bigquery.Table(f"{PROJECT}.{DS}.device_telemetry_history", schema=fact_schema)
fact_tbl.description="단말기 텔레메트리 이력(사실, 30일)"
fact_tbl.time_partitioning=bigquery.TimePartitioning(field="ingest_date")
bq.create_table(fact_tbl, exists_ok=True)

now=datetime.now(timezone.utc); rows=[]
for _ in range(2400):
    d=random.choice(devices); metric=random.choice(list(METRICS))
    lo,hi,unit=METRICS[metric]
    ts=now-timedelta(days=random.randint(0,29),seconds=random.randint(0,86400))
    rows.append({"device_id":d,"event_ts":ts.isoformat(),"metric":metric,
                 "value":round(random.uniform(lo,hi),2),"unit":unit,
                 "battery_pct":random.randint(20,100),"ingest_date":ts.date().isoformat()})
bq.load_table_from_json(rows, f"{PROJECT}.{DS}.device_telemetry_history",
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE",schema=fact_schema)).result()
print(f"device_telemetry_history loaded: {len(rows)} rows")

# 4) 검증
for q in [f"SELECT count(*) c FROM `{PROJECT}.{DS}.device_telemetry_history`",
          f"SELECT metric, count(*) c, round(avg(value),2) avg FROM `{PROJECT}.{DS}.device_telemetry_history` GROUP BY metric ORDER BY c DESC"]:
    print("Q:",q.split("FROM")[0].strip()[:60])
    for r in bq.query(q).result(): print("  ",dict(r))
