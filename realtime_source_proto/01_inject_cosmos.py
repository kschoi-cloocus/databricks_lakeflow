#!/usr/bin/env python3
"""Cosmos DB device_state 주입 — 단말기 참조/hot-state (중립 IoT 샘플, 고객정보 없음)."""
import json, random
from datetime import datetime, timezone, timedelta
from azure.cosmos import CosmosClient

cfg = json.load(open("/mnt/d/googledrive.ai/Work/AI/claude-code-beamrock/.secrets/cosmos.json"))
client = CosmosClient(cfg["endpoint"], cfg["key"])
cont = client.get_database_client(cfg["database"]).get_container_client(cfg["container"])

MODELS  = ["EnvSense-X2", "EnvSense-X3", "AirNode-Pro"]
REGIONS = ["KR-Seoul", "KR-Busan", "JP-Tokyo", "US-East"]
SITES   = ["Plant-A", "Plant-B", "Warehouse-1", "Lab-3"]
now = datetime.now(timezone.utc)

random.seed(42)
n = 0
for i in range(1, 21):
    did = f"SN-{i:04d}"
    doc = {
        "id": did,                      # Cosmos 필수 키
        "device_id": did,               # partition key (/device_id)
        "model": random.choice(MODELS),
        "firmware": random.choice(["2.3.0", "2.4.1", "2.5.0"]),
        "region": random.choice(REGIONS),
        "site": random.choice(SITES),
        "registered_at": (now - timedelta(days=random.randint(30, 400))).isoformat(),
        "status": random.choice(["active", "active", "active", "inactive"]),
        "last_seen": (now - timedelta(minutes=random.randint(0, 120))).isoformat(),
        "battery_pct": random.randint(20, 100),
        "_doc_type": "device_state",
    }
    cont.upsert_item(doc)
    n += 1

print(f"Cosmos device_state upsert 완료: {n} devices (SN-0001 ~ SN-0020)")
# 검증: 몇 건 read back
sample = list(cont.query_items("SELECT c.device_id, c.model, c.region, c.status FROM c OFFSET 0 LIMIT 3",
                               enable_cross_partition_query=True))
for s in sample:
    print("  ", s)
