# Databricks notebook source
# ============================================================================
# Lakeflow SDP (Spark Declarative Pipeline) — 단말기 실시간 프로토타입
#   원천1: Azure Event Hub (device-telemetry)  → 실시간 이벤트 스트리밍(Kafka 호환)
#   원천2: Azure Cosmos DB (device_state)       → 단말기 참조/hot-state (Delta 브리지)
#   메달리온: bronze(raw) → silver(파싱·품질·격리) → silver_enriched(조인) → gold(집계)
#   ※ 중립 IoT 텔레메트리 샘플. 고객/실데이터 없음.
# ============================================================================
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType, LongType)

# ── Event Hub (Kafka 엔드포인트) ─────────────────────────────────────────
BOOTSTRAP = "ehns-iotproto-kc22.servicebus.windows.net:9093"
HUB       = "device-telemetry"
CONN      = dbutils.secrets.get("iot_proto", "eh_conn_str")   # 시크릿에서 로드
JAAS      = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
             f'required username="$ConnectionString" password="{CONN}";')

REF_TABLE = "adb_wrkspc_krc_dev.iot_proto.device_reference"   # Cosmos 스냅샷(브리지)

SCHEMA = StructType([
    StructField("device_id",   StringType()),
    StructField("event_ts",    StringType()),
    StructField("metric",      StringType()),
    StructField("value",       DoubleType()),
    StructField("unit",        StringType()),
    StructField("battery_pct", IntegerType()),
    StructField("rssi",        IntegerType()),
    StructField("seq",         LongType()),
    StructField("_inject_kind", StringType()),
])

# ── Bronze: Event Hub 원천 그대로(스트리밍 테이블) ───────────────────────
@dlt.table(name="bronze_telemetry",
           comment="Event Hub device-telemetry 원천(raw). Kafka 포맷 네이티브 스트리밍",
           table_properties={"quality": "bronze", "source": "azure.eventhub"})
def bronze_telemetry():
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", HUB)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", JAAS)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "2000")
            .option("failOnDataLoss", "false")
            .load()
            .select(F.col("value").cast("string").alias("raw_json"),
                    F.col("partition").alias("eh_partition"),
                    F.col("offset").alias("eh_offset"),
                    F.col("timestamp").alias("eh_enqueued_ts")))

# 품질 규칙(silver 진입 기준) — metric별 물리 유효범위
RANGE_EXPR = ("value IS NOT NULL AND ("
              "(metric='temperature' AND value BETWEEN -50 AND 100) OR "
              "(metric='humidity'    AND value BETWEEN 0 AND 100) OR "
              "(metric='co2'         AND value BETWEEN 300 AND 5000) OR "
              "(metric='vibration'   AND value BETWEEN 0 AND 100))")
RULES = {
    "valid_device_id":       "device_id IS NOT NULL",
    "value_in_metric_range": RANGE_EXPR,
    "battery_non_negative":  "battery_pct >= 0",
}

# ── Silver: 파싱 + 품질통과(위반 drop) ───────────────────────────────────
@dlt.table(name="silver_telemetry",
           comment="JSON 파싱 + 품질규칙 통과 텔레메트리",
           table_properties={"quality": "silver"})
@dlt.expect_all_or_drop(RULES)
def silver_telemetry():
    return (dlt.read_stream("bronze_telemetry")
            .select(F.from_json("raw_json", SCHEMA).alias("j"),
                    "eh_partition", "eh_offset", "eh_enqueued_ts")
            .select("j.*", "eh_partition", "eh_offset", "eh_enqueued_ts")
            .withColumn("event_ts", F.to_timestamp("event_ts"))
            .drop("_inject_kind"))

# ── Silver 격리: 품질 위반 이벤트 별도 보관 ──────────────────────────────
@dlt.table(name="silver_quarantine",
           comment="품질 위반(누락 ID/범위초과/음수배터리) 격리 테이블",
           table_properties={"quality": "silver"})
def silver_quarantine():
    return (dlt.read_stream("bronze_telemetry")
            .select(F.from_json("raw_json", SCHEMA).alias("j"), "eh_partition", "eh_offset")
            .select("j.*", "eh_partition", "eh_offset")
            .filter(f"NOT(device_id IS NOT NULL AND ({RANGE_EXPR}) AND battery_pct >= 0)")
            .withColumn("quarantine_reason",
                        F.when(F.col("device_id").isNull(), "null_device_id")
                         .when(F.expr(f"NOT({RANGE_EXPR})"), "value_out_of_range")
                         .when(F.col("battery_pct") < 0, "negative_battery")
                         .otherwise("other")))

# ── Silver enriched: Cosmos device_reference 조인(stream-static) ──────────
@dlt.table(name="silver_enriched",
           comment="텔레메트리 + Cosmos 단말기 참조(model/region/site/status) 조인",
           table_properties={"quality": "silver"})
def silver_enriched():
    tele = dlt.read_stream("silver_telemetry")
    ref  = spark.read.table(REF_TABLE)
    return (tele.join(F.broadcast(ref), "device_id", "left")
            .select(tele["device_id"], tele["event_ts"], tele["metric"], tele["value"],
                    tele["unit"], tele["battery_pct"], tele["rssi"],
                    ref["model"], ref["region"], ref["site"],
                    ref["status"].alias("device_status")))

# ── Gold: region × metric 집계(materialized view) ────────────────────────
@dlt.table(name="gold_region_metric",
           comment="region×metric 텔레메트리 집계(운영 대시보드용)",
           table_properties={"quality": "gold"})
def gold_region_metric():
    return (dlt.read("silver_enriched")
            .groupBy("region", "metric")
            .agg(F.count("*").alias("readings"),
                 F.round(F.avg("value"), 2).alias("avg_value"),
                 F.min("value").alias("min_value"),
                 F.max("value").alias("max_value"),
                 F.round(F.avg("battery_pct"), 1).alias("avg_battery"),
                 F.countDistinct("device_id").alias("active_devices")))

# ── Gold: 단말기별 최신 상태 요약 ────────────────────────────────────────
@dlt.table(name="gold_device_summary",
           comment="단말기별 관측 요약 + Cosmos 상태",
           table_properties={"quality": "gold"})
def gold_device_summary():
    return (dlt.read("silver_enriched")
            .groupBy("device_id", "model", "region", "device_status")
            .agg(F.count("*").alias("readings"),
                 F.round(F.avg("battery_pct"), 1).alias("avg_battery"),
                 F.max("event_ts").alias("last_event_ts")))
