#!/usr/bin/env python3
"""Event Hub device-telemetry 주입 — 단말기 실시간 텔레메트리 (중립 IoT 샘플).
   품질 시나리오 실증을 위해 ~6% 불량 이벤트(null id / 범위초과 / 음수배터리) 혼입."""
import json, random
from datetime import datetime, timezone, timedelta
from azure.eventhub import EventHubProducerClient, EventData

cfg = json.load(open("/mnt/d/googledrive.ai/Work/AI/claude-code-beamrock/.secrets/eventhub.json"))
producer = EventHubProducerClient.from_connection_string(cfg["connection_string"], eventhub_name=cfg["hub"])

METRICS = {  # metric -> (unit, good_lo, good_hi)
    "temperature": ("C",  -10.0, 60.0),
    "humidity":    ("pct",  0.0, 100.0),
    "co2":         ("ppm", 350.0, 2000.0),
    "vibration":   ("mm_s", 0.0, 50.0),
}
DEVICES = [f"SN-{i:04d}" for i in range(1, 21)]
now = datetime.now(timezone.utc)

random.seed(7)
TOTAL = 600
events, seq = [], 1000
for k in range(TOTAL):
    did = random.choice(DEVICES)
    metric = random.choice(list(METRICS))
    unit, lo, hi = METRICS[metric]
    ts = now - timedelta(seconds=random.randint(0, 3600))
    val = round(random.uniform(lo, hi), 2)
    batt = random.randint(15, 100)
    kind = "good"

    r = random.random()
    if r < 0.02:                      # 불량 A: device_id 누락
        did, kind = None, "null_id"
    elif r < 0.045:                   # 불량 B: 범위 초과 값
        val, kind = round(hi * random.uniform(3, 6), 2), "out_of_range"
    elif r < 0.06:                    # 불량 C: 음수 배터리
        batt, kind = -random.randint(1, 30), "neg_battery"

    evt = {
        "device_id": did,
        "event_ts": ts.isoformat(),
        "metric": metric,
        "value": val,
        "unit": unit,
        "battery_pct": batt,
        "rssi": -random.randint(40, 110),
        "seq": seq,
        "_inject_kind": kind,          # 데모 확인용(파이프라인은 사용 안 함)
    }
    events.append(evt)
    seq += 1

# 배치 전송
sent, batch = 0, producer.create_batch()
with producer:
    for e in events:
        data = EventData(json.dumps(e))
        try:
            batch.add(data)
        except ValueError:
            producer.send_batch(batch); sent += len(batch); batch = producer.create_batch(); batch.add(data)
    if len(batch):
        producer.send_batch(batch); sent += len(batch)

kinds = {}
for e in events:
    kinds[e["_inject_kind"]] = kinds.get(e["_inject_kind"], 0) + 1
print(f"Event Hub 전송 완료: {sent} events → {cfg['hub']}")
print("  주입 구성:", kinds)
