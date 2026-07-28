#!/usr/bin/env python3
# Phase 3 — GCP 거버넌스 자산 읽기 → gov_inventory.json
import warnings, json, time; warnings.filterwarnings("ignore")
from google.oauth2 import service_account
from google.cloud import bigquery, dataplex_v1, datacatalog_lineage_v1
from google.api_core.exceptions import ResourceExhausted
KEY="<PATH>/google-sa.json"
PROJ="myproject-493803"; PNUM="803880923651"; LOC="asia-northeast3"; DS="health_demo"
creds=service_account.Credentials.from_service_account_file(KEY)
def call(fn,*a,**k):
    for _ in range(6):
        try: return fn(*a,**k)
        except ResourceExhausted: time.sleep(20)
    raise RuntimeError("quota")

inv={"metadata":{}, "aspects":{}, "data_quality":{}, "lineage":[]}

# 1) BQ 메타(설명·라벨·컬럼설명)
bq=bigquery.Client(project=PROJ, credentials=creds)
for t in ["patients","vitals","vitals_daily"]:
    tb=bq.get_table(f"{PROJ}.{DS}.{t}")
    inv["metadata"][t]={"description":tb.description,"labels":dict(tb.labels or {}),
        "columns":{f.name:f.description for f in tb.schema}}

# 2) Dataplex aspect (data-classification on patients)
cat=dataplex_v1.CatalogServiceClient(credentials=creds)
eg=f"projects/{PROJ}/locations/{LOC}/entryGroups/health-catalog"
e=call(cat.get_entry, request=dataplex_v1.GetEntryRequest(name=f"{eg}/entries/patients",
    view="CUSTOM", aspect_types=[f"projects/{PROJ}/locations/{LOC}/aspectTypes/data-classification"]))
for k,a in e.aspects.items():
    inv["aspects"]["patients"]={"aspect_type":k, "data":dict(a.data)}

# 3) DataScan 규칙 + 최신 결과
dsc=dataplex_v1.DataScanServiceClient(credentials=creds)
sname=f"projects/{PROJ}/locations/{LOC}/dataScans/vitals-dq"
s=call(dsc.get_data_scan, request=dataplex_v1.GetDataScanRequest(name=sname, view="FULL"))
rules=[]
for r in s.data_quality_spec.rules:
    kind=next((f for f in ["non_null_expectation","range_expectation","set_expectation","regex_expectation"] if r._pb.HasField(f)),"?")
    d={"column":r.column,"dimension":r.dimension,"type":kind}
    if kind=="range_expectation": d["min"]=r.range_expectation.min_value; d["max"]=r.range_expectation.max_value
    rules.append(d)
jobs=list(call(dsc.list_data_scan_jobs, parent=sname))
res={}
if jobs:
    j=call(dsc.get_data_scan_job, request=dataplex_v1.GetDataScanJobRequest(name=jobs[0].name, view="FULL"))
    res={"passed":j.data_quality_result.passed,"row_count":j.data_quality_result.row_count,
         "rules":[{"column":rr.rule.column,"passed":rr.passed,"pass_ratio":round(rr.pass_ratio,2)} for rr in j.data_quality_result.rules]}
inv["data_quality"]={"target":"vitals","rules":rules,"latest_result":res}

# 4) Lineage (vitals → vitals_daily)
lin=datacatalog_lineage_v1.LineageClient(credentials=creds)
try:
    req=datacatalog_lineage_v1.SearchLinksRequest(parent=f"projects/{PROJ}/locations/{LOC}",
        target=datacatalog_lineage_v1.EntityReference(fully_qualified_name=f"bigquery:{PROJ}.{DS}.vitals_daily"))
    for link in lin.search_links(request=req):
        inv["lineage"].append({"source":link.source.fully_qualified_name,"target":link.target.fully_qualified_name})
except Exception as ex:
    inv["lineage_note"]=str(ex)[:150]

open("gov_inventory.json","w").write(json.dumps(inv,ensure_ascii=False,indent=2))
print(json.dumps(inv,ensure_ascii=False,indent=2))
