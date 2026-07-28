#!/usr/bin/env python3
# 02 — Dataplex DataScan(데이터 품질) 생성 + 실행 + 결과 (FULL view)
# 주의: Dataplex API는 분당 요청 쿼터(리전당 ~10)가 있어 호출 간격/백오프 필요.
import warnings, time; warnings.filterwarnings("ignore")
from google.oauth2 import service_account
from google.cloud import dataplex_v1
from google.api_core.exceptions import ResourceExhausted, AlreadyExists

KEY="<PATH>/google-sa.json"           # BigQuery+Dataplex 권한 SA 키
PROJ="myproject-493803"; LOC="asia-northeast3"; DS="health_demo"
creds=service_account.Credentials.from_service_account_file(KEY)
c=dataplex_v1.DataScanServiceClient(credentials=creds)
parent=f"projects/{PROJ}/locations/{LOC}"; scan_id="vitals-dq"
name=f"{parent}/dataScans/{scan_id}"

def call(fn,*a,**k):
    for _ in range(6):
        try: return fn(*a,**k)
        except ResourceExhausted: print("  (rate limit 20s)"); time.sleep(20)
        except AlreadyExists: return "EXISTS"
    raise RuntimeError("quota")

R=dataplex_v1.DataQualityRule
rules=[
    R(column="patient_id", dimension="COMPLETENESS", non_null_expectation=R.NonNullExpectation()),
    R(column="heart_rate", dimension="VALIDITY", range_expectation=R.RangeExpectation(min_value="20", max_value="300")),
    R(column="systolic",   dimension="VALIDITY", range_expectation=R.RangeExpectation(min_value="50", max_value="250")),
]
scan=dataplex_v1.DataScan()
scan.data.resource=f"//bigquery.googleapis.com/projects/{PROJ}/datasets/{DS}/tables/vitals"
scan.data_quality_spec=dataplex_v1.DataQualitySpec(rules=rules)

r=call(c.create_data_scan, parent=parent, data_scan=scan, data_scan_id=scan_id)
if r not in (None,"EXISTS"): r.result()
print("DataScan:", scan_id, f"(rules={len(rules)})")

# ACTIVE 대기
for _ in range(12):
    s=call(c.get_data_scan, name=name)
    if dataplex_v1.State(s.state).name=="ACTIVE": break
    time.sleep(15)
# 실행 + 결과
job=call(c.run_data_scan, name=name).job; time.sleep(10)
for _ in range(15):
    j=call(c.get_data_scan_job, request=dataplex_v1.GetDataScanJobRequest(name=job.name, view="FULL"))
    if dataplex_v1.DataScanJob.State(j.state).name in ("SUCCEEDED","FAILED","CANCELLED"): break
    time.sleep(15)
res=j.data_quality_result
print(f"결과: passed={res.passed} rows={res.row_count}")
for rr in res.rules:
    print(f"  {rr.rule.column:12s} passed={rr.passed} passRatio={rr.pass_ratio:.2f}")
