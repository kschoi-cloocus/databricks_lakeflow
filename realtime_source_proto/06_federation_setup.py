#!/usr/bin/env python3
"""Databricks Lakehouse Federation → BigQuery 연결·조회·Delta 이관 (Part A)."""
import json, subprocess, time
P="DEFAULT"; WID="e6d81062ddc9efdc"; CAT="adb_wrkspc_krc_dev"; SCH="iot_proto"
BQ_PROJECT="myproject-493803"; BQ_DS="iot_proto"
CONN="bq_iot_conn"; FCAT="bq_iot_fed"
KEY="/mnt/d/googledrive.ai/Work/AI/claude-code-beamrock/.secrets/google-sa.json"

def cli(args, inp=None):
    return subprocess.run(["databricks"]+args+["-p",P], input=inp, capture_output=True, text=True)
def api(method, path, body=None):
    a=["api",method,path]+(["--json",json.dumps(body)] if body is not None else [])
    r=cli(a)
    try: return json.loads(r.stdout) if r.stdout.strip() else {"_err":r.stderr}
    except: return {"_raw":r.stdout[:300],"_err":r.stderr[:300]}

# 1) UC Connection (BigQuery) — API로 생성(SQL 이스케이프 회피)
sa_str=json.dumps(json.load(open(KEY)))
api("delete", f"/api/2.1/unity-catalog/connections/{CONN}")  # 재실행 대비
c=api("post","/api/2.1/unity-catalog/connections",
      {"name":CONN,"connection_type":"BIGQUERY","options":{"GoogleServiceAccountKeyJson":sa_str}})
print("1) connection:", c.get("name") or c.get("_err") or c)

# 2) Foreign Catalog
api("delete", f"/api/2.1/unity-catalog/catalogs/{FCAT}?force=true")
fc=api("post","/api/2.1/unity-catalog/catalogs",
       {"name":FCAT,"connection_name":CONN,"options":{"dataProjectId":BQ_PROJECT}})
print("2) foreign catalog:", fc.get("name") or fc.get("_err") or fc)

# ── SQL 실행 헬퍼 ──
def sql(stmt, catalog=CAT, schema=SCH):
    body={"warehouse_id":WID,"catalog":catalog,"schema":schema,"statement":stmt,"wait_timeout":"50s"}
    d=json.loads(cli(["api","post","/api/2.0/sql/statements","--json",json.dumps(body)]).stdout)
    sid=d.get("statement_id")
    for _ in range(25):
        if (d.get("status") or {}).get("state") in ("SUCCEEDED","FAILED","CANCELED"): break
        time.sleep(3)
        d=json.loads(cli(["api","get",f"/api/2.0/sql/statements/{sid}"]).stdout)
    return d
def rows(d): return (d.get("result") or {}).get("data_array") or []
def state(d): return (d.get("status") or {}).get("state"), ((d.get("status") or {}).get("error") or {}).get("message","")

# 3) 페더레이션 라이브 조회
print("\n3) BigQuery 라이브 조회 (federation, 데이터 이동 없음)")
d=sql(f"SELECT count(*) FROM {FCAT}.{BQ_DS}.device_telemetry_history")
st,err=state(d)
print("   fact count:", rows(d) if st=="SUCCEEDED" else f"{st} / {err[:160]}")
if st!="SUCCEEDED":
    print("   >> 페더레이션 실패 — fallback(export) 필요 신호"); raise SystemExit(0)
print("   catalog count:", rows(sql(f"SELECT count(*) FROM {FCAT}.{BQ_DS}.device_catalog")))

# 4) Delta 이관 (batch replatform = federation CTAS)
print("\n4) Delta 이관 (BigQuery → Delta, medallion bronze)")
for src,tgt in [("device_telemetry_history","bq_telemetry_bronze"),("device_catalog","bq_device_catalog")]:
    d=sql(f"CREATE OR REPLACE TABLE {CAT}.{SCH}.{tgt} AS SELECT * FROM {FCAT}.{BQ_DS}.{src}")
    st,err=state(d); print(f"   {src:26s} → {tgt:22s} {st} {err[:80]}")
sql(f"ALTER TABLE {CAT}.{SCH}.bq_telemetry_bronze SET TBLPROPERTIES ('source'='bigquery.federation','quality'='bronze')")

# 5) 검증 + BigQuery↔Delta 행수 대조
print("\n5) 이관 검증 (BigQuery ↔ Delta 대조)")
d=sql(f"SELECT count(*) FROM {CAT}.{SCH}.bq_telemetry_bronze"); print("   Delta bronze rows:", rows(d))
d=sql(f"""SELECT c.region, count(*) readings, round(avg(t.value),2) avg_val
          FROM {CAT}.{SCH}.bq_telemetry_bronze t
          JOIN {CAT}.{SCH}.bq_device_catalog c USING(device_id)
          GROUP BY c.region ORDER BY readings DESC""")
print("   region 집계(Delta 조인):")
for r in rows(d): print("    ",r)
print("\nDONE")
