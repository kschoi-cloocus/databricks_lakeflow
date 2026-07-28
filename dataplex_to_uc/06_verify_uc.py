#!/usr/bin/env python3
# Phase 4 aspect 태그 수정 + Phase 5 검증 (UC information_schema / system.access)
import json, subprocess, time
P="DEFAULT"; WID="e6d81062ddc9efdc"; CAT="adb_wrkspc_krc_dev"; SCH="gov_demo"
def sql(stmt, silent=False):
    body={"warehouse_id":WID,"catalog":CAT,"schema":SCH,"statement":stmt,"wait_timeout":"50s"}
    p=subprocess.run(["databricks","api","post","/api/2.0/sql/statements","--json",json.dumps(body),"-p",P],
                     capture_output=True,text=True)
    if p.returncode!=0 or not p.stdout.strip(): return {"_err":p.stderr[:200] or "empty"}
    d=json.loads(p.stdout); sid=d.get("statement_id")
    for _ in range(20):
        if (d.get("status") or {}).get("state") in ("SUCCEEDED","FAILED","CANCELED","CLOSED"): break
        time.sleep(3)
        g=subprocess.run(["databricks","api","get",f"/api/2.0/sql/statements/{sid}","-p",P],capture_output=True,text=True); d=json.loads(g.stdout)
    return d
def rows(d): return (d.get("result") or {}).get("data_array") or []

# ── aspect 태그 수정(언더스코어 키) ──
print("=== aspect → 태그(수정: 언더스코어 키) ===")
d=sql("ALTER TABLE patients SET TAGS ('data_classification_sensitivity'='high','data_classification_pii_category'='PHI','data_classification_steward'='data-team')")
print("  ", (d.get("status") or {}).get("state"))

print("\n"+"="*64)
print("Phase 5 검증 — GCP Dataplex 자산이 UC에 이관됐는지 (system/information_schema)")
print("="*64)

print("\n① 메타데이터 — 컬럼 설명(comment) 이관")
for r in rows(sql("SELECT table_name, column_name, comment FROM information_schema.columns WHERE table_schema='gov_demo' AND comment IS NOT NULL ORDER BY table_name, ordinal_position", silent=True)):
    print(f"   {r[0]}.{r[1]:12s} → {r[2]}")

print("\n② 태그 — 라벨/aspect → UC 테이블 태그")
for r in rows(sql("SELECT table_name, tag_name, tag_value FROM information_schema.table_tags WHERE schema_name='gov_demo' ORDER BY table_name, tag_name", silent=True)):
    print(f"   {r[0]:14s} {r[1]:32s} = {r[2]}")

print("\n② 컬럼 태그 — PHI 분류")
for r in rows(sql("SELECT table_name, column_name, tag_name, tag_value FROM information_schema.column_tags WHERE schema_name='gov_demo' ORDER BY table_name, column_name", silent=True)):
    print(f"   {r[0]}.{r[1]:12s} {r[2]}={r[3]}")

print("\n④ 데이터 품질 — DataScan 규칙 → Delta 제약")
for r in rows(sql("SELECT constraint_name, check_clause FROM information_schema.check_constraints WHERE constraint_schema='gov_demo' ORDER BY constraint_name", silent=True)):
    print(f"   {r[0]:20s} {r[1]}")
for r in rows(sql("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema='gov_demo' AND is_nullable='NO' ORDER BY table_name", silent=True)):
    print(f"   NOT NULL: {r[0]}.{r[1]}")

print("\n⑤ 리니지 — CTAS 자동 캡처 (system.access.table_lineage)")
d=sql("SELECT source_table_full_name, target_table_full_name FROM system.access.table_lineage WHERE target_table_full_name = 'adb_wrkspc_krc_dev.gov_demo.vitals_daily' AND source_table_full_name IS NOT NULL GROUP BY 1,2", silent=True)
if d.get("_err") or (d.get("status",{}) or {}).get("state")!="SUCCEEDED":
    print("   (system.access 미활성 또는 리니지 지연 — UC UI에서도 확인 가능)")
else:
    rr=rows(d)
    if rr:
        for r in rr: print(f"   {r[0]} → {r[1]}")
    else:
        print("   (리니지 아직 미표출 — 수집 지연, 잠시 후 재조회)")
print("\nDONE")
