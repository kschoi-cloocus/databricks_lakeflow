#!/usr/bin/env python3
# Phase 4 — GCP 거버넌스 인벤토리 → Unity Catalog 이관
import json, subprocess, time
INV=json.load(open("gov_inventory.json"))
P="DEFAULT"; WID="e6d81062ddc9efdc"; CAT="adb_wrkspc_krc_dev"; SCH="gov_demo"

def sql(stmt):
    body={"warehouse_id":WID,"catalog":CAT,"schema":SCH,"statement":stmt,"wait_timeout":"50s"}
    p=subprocess.run(["databricks","api","post","/api/2.0/sql/statements","--json",json.dumps(body),"-p",P],
                     capture_output=True,text=True)
    if p.returncode!=0 or not p.stdout.strip(): return {"_err":p.stderr[:200] or "empty"}
    d=json.loads(p.stdout); sid=d.get("statement_id")
    for _ in range(20):
        st=(d.get("status") or {}).get("state")
        if st in ("SUCCEEDED","FAILED","CANCELED","CLOSED"): break
        time.sleep(3)
        g=subprocess.run(["databricks","api","get",f"/api/2.0/sql/statements/{sid}","-p",P],capture_output=True,text=True)
        d=json.loads(g.stdout)
    st=(d.get("status") or {}).get("state"); err=((d.get("status") or {}).get("error") or {}).get("message","")
    tag="OK " if st=="SUCCEEDED" else "ERR"
    print(f"  [{tag}] {stmt.strip().splitlines()[0][:66]}"+(f"  <-{err[:80]}" if err else ""))
    return d

esc=lambda s:(s or "").replace("'","''")

print("=== 스키마·테이블(미러) ===")
sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")
sql("CREATE OR REPLACE TABLE patients (patient_id STRING, name STRING, birth_year INT, region STRING)")
sql("INSERT INTO patients VALUES ('P001','Alice',1980,'Seoul'),('P002','Bob',1975,'Busan'),('P003','Carol',1990,'Daegu'),('P004','Dave',1985,'Incheon')")
sql("CREATE OR REPLACE TABLE vitals (patient_id STRING, measured_at TIMESTAMP, heart_rate INT, systolic INT, diastolic INT)")
sql("INSERT INTO vitals VALUES ('P001',TIMESTAMP'2026-07-01 09:00:00',72,120,80),('P001',TIMESTAMP'2026-07-02 09:00:00',75,122,79),('P002',TIMESTAMP'2026-07-01 09:00:00',68,118,76),('P003',TIMESTAMP'2026-07-01 09:00:00',88,130,85),('P004',TIMESTAMP'2026-07-01 09:00:00',95,140,90)")

print("=== ① 메타데이터: COMMENT (설명) ===")
for t,m in INV["metadata"].items():
    if t=="vitals_daily": continue
    if m["description"]: sql(f"COMMENT ON TABLE {t} IS '{esc(m['description'])}'")
    for col,desc in m["columns"].items():
        if desc: sql(f"ALTER TABLE {t} ALTER COLUMN {col} COMMENT '{esc(desc)}'")

print("=== ② 태그: 라벨 → 테이블 태그 ===")
for t,m in INV["metadata"].items():
    if t=="vitals_daily": continue
    if m["labels"]:
        kv=",".join(f"'{k}'='{v}'" for k,v in m["labels"].items())
        sql(f"ALTER TABLE {t} SET TAGS ({kv})")
    # 컬럼 설명에 PHI 포함 → 컬럼 태그 classification=PHI
    for col,desc in m["columns"].items():
        if desc and "PHI" in desc: sql(f"ALTER TABLE {t} ALTER COLUMN {col} SET TAGS ('classification'='PHI')")

print("=== ③ Aspect → 태그(평탄화) ===")
for t,a in INV["aspects"].items():
    at=a["aspect_type"].split(".")[-1].replace("-","_")  # data_classification
    kv=",".join(f"'{at}_{k}'='{esc(str(v))}'" for k,v in a["data"].items())
    sql(f"ALTER TABLE {t} SET TAGS ({kv})")

print("=== ④ 데이터 품질 → Delta 제약 ===")
for r in INV["data_quality"]["rules"]:
    tgt=INV["data_quality"]["target"]; col=r["column"]
    if r["type"]=="non_null_expectation":
        sql(f"ALTER TABLE {tgt} ALTER COLUMN {col} SET NOT NULL")
    elif r["type"]=="range_expectation":
        sql(f"ALTER TABLE {tgt} ADD CONSTRAINT chk_{col} CHECK ({col} BETWEEN {r['min']} AND {r['max']})")

print("=== ⑤ 리니지: CTAS (vitals→vitals_daily, UC 자동 캡처) ===")
sql("CREATE OR REPLACE TABLE vitals_daily AS SELECT patient_id, date(measured_at) d, round(avg(heart_rate),1) avg_hr, max(systolic) max_sys FROM vitals GROUP BY patient_id, date(measured_at)")
sql("COMMENT ON TABLE vitals_daily IS '환자·일자별 활력징후 집계 (파생)'")
sql("ALTER TABLE vitals_daily SET TAGS ('data_domain'='health','layer'='gold')")
print("Phase 4 완료")
