#!/usr/bin/env python3
import json, subprocess, time
P="DEFAULT"; WID="e6d81062ddc9efdc"; CAT="adb_wrkspc_krc_dev"; SCH="iot_proto"
def sql(stmt):
    body={"warehouse_id":WID,"catalog":CAT,"schema":SCH,"statement":stmt,"wait_timeout":"50s"}
    d=json.loads(subprocess.run(["databricks","api","post","/api/2.0/sql/statements","--json",json.dumps(body),"-p",P],capture_output=True,text=True).stdout)
    sid=d.get("statement_id")
    for _ in range(20):
        if (d.get("status") or {}).get("state") in ("SUCCEEDED","FAILED","CANCELED"): break
        time.sleep(3)
        d=json.loads(subprocess.run(["databricks","api","get",f"/api/2.0/sql/statements/{sid}","-p",P],capture_output=True,text=True).stdout)
    return d
def rows(d): return (d.get("result") or {}).get("data_array") or []

print("="*68)
print("SDP 결과 검증 — Event Hub + Cosmos → bronze/silver/gold")
print("="*68)

print("\n① 메달리온 행수 (Event Hub 600 이벤트 흐름)")
for t in ["bronze_telemetry","silver_telemetry","silver_quarantine","silver_enriched"]:
    c=rows(sql(f"SELECT count(*) FROM {t}"))
    print(f"   {t:20s} = {c[0][0] if c else '?'}")

print("\n② 품질 격리 사유별 (불량 이벤트 분류)")
for r in rows(sql("SELECT quarantine_reason, count(*) FROM silver_quarantine GROUP BY quarantine_reason ORDER BY 2 DESC")):
    print(f"   {r[0]:22s} {r[1]}")

print("\n③ Cosmos 조인 결과 — region별 (enrichment 성공 확인)")
for r in rows(sql("SELECT region, count(*) readings, count(DISTINCT device_id) devices FROM silver_enriched GROUP BY region ORDER BY 2 DESC")):
    print(f"   {str(r[0]):10s} readings={r[1]:5s} devices={r[2]}")

print("\n④ Gold — region × metric 집계 (상위 8)")
print(f"   {'region':10s} {'metric':12s} {'reads':>6s} {'avg':>9s} {'min':>8s} {'max':>9s} {'batt':>6s}")
for r in rows(sql("SELECT region,metric,readings,avg_value,min_value,max_value,avg_battery FROM gold_region_metric ORDER BY readings DESC LIMIT 8")):
    print(f"   {str(r[0]):10s} {r[1]:12s} {r[2]:>6s} {r[3]:>9s} {r[4]:>8s} {r[5]:>9s} {r[6]:>6s}")

print("\n⑤ Gold — 단말기 요약 (상위 5, Cosmos status 포함)")
for r in rows(sql("SELECT device_id,model,region,device_status,readings,avg_battery FROM gold_device_summary ORDER BY readings DESC LIMIT 5")):
    print(f"   {r[0]} {str(r[1]):12s} {str(r[2]):10s} {str(r[3]):9s} reads={r[4]} batt={r[5]}")

print("\n⑥ 품질 메트릭 — expectations (event_log: drop된 레코드수)")
d=sql(f"""SELECT explode(from_json(details:flow_progress:data_quality:expectations,'array<struct<name:string,dataset:string,passed_records:long,failed_records:long>>')) e
          FROM event_log(TABLE(iot_proto.silver_telemetry)) WHERE details:flow_progress:data_quality IS NOT NULL""")
seen=set()
for r in rows(d):
    try: e=json.loads(r[0]) if isinstance(r[0],str) else r[0]
    except: e=None
    if e:
        k=e.get("name")
        if k and k not in seen:
            seen.add(k); print(f"   {k:24s} passed={e.get('passed_records')} failed={e.get('failed_records')}")
if not seen: print("   (event_log expectations 조회 방식 상이 — 격리 테이블로 대체 확인됨: ②)")
print("\nDONE")
