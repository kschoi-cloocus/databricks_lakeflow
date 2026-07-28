#!/usr/bin/env python3
# Phase 1 — BigQuery 샘플 구성 (설명·라벨·파생테이블 lineage)
import warnings; warnings.filterwarnings("ignore")
from google.oauth2 import service_account
from google.cloud import bigquery

KEY="<PATH>/google-sa.json"
PROJ="myproject-493803"; LOC="asia-northeast3"; DS="health_demo"
creds=service_account.Credentials.from_service_account_file(KEY)
bq=bigquery.Client(project=PROJ, credentials=creds)

# 1) 데이터셋
ds=bigquery.Dataset(f"{PROJ}.{DS}"); ds.location=LOC
ds.description="건강데이터 거버넌스 이관 데모 (Dataplex→UC)"
ds.labels={"data_domain":"health","env":"demo"}
bq.create_dataset(ds, exists_ok=True)
print("데이터셋:", DS)

# 2) patients (설명 + 라벨)
patients=bigquery.Table(f"{PROJ}.{DS}.patients", schema=[
    bigquery.SchemaField("patient_id","STRING",description="환자 식별자 (PHI)"),
    bigquery.SchemaField("name","STRING",description="환자 이름 (PHI)"),
    bigquery.SchemaField("birth_year","INT64",description="출생연도"),
    bigquery.SchemaField("region","STRING",description="거주 지역"),
])
patients.description="환자 마스터 (민감정보 포함)"
patients.labels={"data_domain":"health","pii":"true","layer":"bronze"}
bq.create_table(patients, exists_ok=True)

# 3) vitals (설명)
vitals=bigquery.Table(f"{PROJ}.{DS}.vitals", schema=[
    bigquery.SchemaField("patient_id","STRING",description="환자 식별자 (PHI)"),
    bigquery.SchemaField("measured_at","TIMESTAMP",description="측정 시각"),
    bigquery.SchemaField("heart_rate","INT64",description="심박수 (bpm)"),
    bigquery.SchemaField("systolic","INT64",description="수축기 혈압"),
    bigquery.SchemaField("diastolic","INT64",description="이완기 혈압"),
])
vitals.description="활력징후 측정 원천"
vitals.labels={"data_domain":"health","pii":"true","layer":"bronze"}
bq.create_table(vitals, exists_ok=True)
print("테이블: patients, vitals 생성(설명·라벨 포함)")

# 4) 데이터 적재
bq.query(f"""DELETE FROM {DS}.patients WHERE TRUE""").result()
bq.query(f"""INSERT INTO {DS}.patients VALUES
 ('P001','Alice',1980,'Seoul'),('P002','Bob',1975,'Busan'),
 ('P003','Carol',1990,'Daegu'),('P004','Dave',1985,'Incheon')""").result()
bq.query(f"""DELETE FROM {DS}.vitals WHERE TRUE""").result()
bq.query(f"""INSERT INTO {DS}.vitals VALUES
 ('P001',TIMESTAMP'2026-07-01 09:00:00',72,120,80),
 ('P001',TIMESTAMP'2026-07-02 09:00:00',75,122,79),
 ('P002',TIMESTAMP'2026-07-01 09:00:00',68,118,76),
 ('P003',TIMESTAMP'2026-07-01 09:00:00',88,130,85),
 ('P004',TIMESTAMP'2026-07-01 09:00:00',95,140,90)""").result()

# 5) 파생 테이블 (CTAS) → lineage vitals→vitals_daily 자동 생성
bq.query(f"""CREATE OR REPLACE TABLE {DS}.vitals_daily AS
 SELECT patient_id, DATE(measured_at) AS d,
        ROUND(AVG(heart_rate),1) AS avg_hr,
        MAX(systolic) AS max_sys
 FROM {DS}.vitals GROUP BY patient_id, DATE(measured_at)""").result()
bq.update_table(bigquery.Table.from_api_repr({**bq.get_table(f"{PROJ}.{DS}.vitals_daily").to_api_repr(),
    "description":"환자·일자별 활력징후 집계 (파생)","labels":{"data_domain":"health","layer":"gold"}}),
    ["description","labels"])
print("파생: vitals_daily (CTAS) → lineage 생성")

# 확인
for t in ["patients","vitals","vitals_daily"]:
    tb=bq.get_table(f"{PROJ}.{DS}.{t}")
    print(f"  {t}: rows≈{tb.num_rows}, desc='{tb.description}', labels={tb.labels}")
print("Phase 1 완료")
