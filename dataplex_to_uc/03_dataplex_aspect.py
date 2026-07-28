#!/usr/bin/env python3
# 03 — Dataplex Catalog: AspectType + EntryGroup + Entry(+Aspect)
# 구조화 메타(Aspect Type) → 나중에 UC 태그로 평탄화 이관되는 대상.
import warnings, time; warnings.filterwarnings("ignore")
from google.oauth2 import service_account
from google.cloud import dataplex_v1
from google.api_core.exceptions import ResourceExhausted, AlreadyExists
from google.protobuf import struct_pb2

KEY="<PATH>/google-sa.json"
PROJ="myproject-493803"; PNUM="803880923651"; LOC="asia-northeast3"   # PNUM=프로젝트 번호
creds=service_account.Credentials.from_service_account_file(KEY)
c=dataplex_v1.CatalogServiceClient(credentials=creds)
parent=f"projects/{PROJ}/locations/{LOC}"

def call(fn,*a,**k):
    for _ in range(6):
        try: return fn(*a,**k)
        except ResourceExhausted: print("  (rate limit 20s)"); time.sleep(20)
        except AlreadyExists: return "EXISTS"
    raise RuntimeError("quota")
def struct(d): s=struct_pb2.Struct(); s.update(d); return s

MT=dataplex_v1.AspectType.MetadataTemplate
# 1) AspectType (구조화 필드)
at=dataplex_v1.AspectType(description="데이터 분류",
    metadata_template=MT(type_="record", name="data_classification", record_fields=[
        MT(name="sensitivity", type_="string", index=1),
        MT(name="pii_category", type_="string", index=2),
        MT(name="steward",      type_="string", index=3)]))
r=call(c.create_aspect_type, parent=parent, aspect_type=at, aspect_type_id="data-classification")
if r not in (None,"EXISTS"): r.result()
print("① AspectType: data-classification"); time.sleep(8)

# 2) EntryGroup
r=call(c.create_entry_group, parent=parent, entry_group=dataplex_v1.EntryGroup(description="건강 카탈로그"), entry_group_id="health-catalog")
if r not in (None,"EXISTS"): r.result()
print("② EntryGroup: health-catalog"); time.sleep(8)

# 3) Entry (필수 generic aspect + 커스텀 data-classification aspect)
eg=f"{parent}/entryGroups/health-catalog"
aspects={
  "dataplex-types.global.generic": dataplex_v1.Aspect(
      aspect_type="projects/dataplex-types/locations/global/aspectTypes/generic", data=struct({})),
  f"{PNUM}.{LOC}.data-classification": dataplex_v1.Aspect(
      aspect_type=f"{parent}/aspectTypes/data-classification",
      data=struct({"sensitivity":"high","pii_category":"PHI","steward":"data-team"})),
}
entry=dataplex_v1.Entry(entry_type="projects/dataplex-types/locations/global/entryTypes/generic", aspects=aspects)
call(c.create_entry, parent=eg, entry=entry, entry_id="patients")
print("③ Entry: patients (+aspect sensitivity/pii_category/steward)")
