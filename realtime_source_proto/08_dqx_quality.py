# Databricks notebook source
# ============================================================================
# DQX (Databricks Labs Data Quality Framework) — SDP expectations 보완 데모
#   목적: 이미 적재된 bronze 데이터를 SDP 파이프라인 "밖에서"(at-rest) 검증
#   차별점: ①행/열 상세진단(_errors/_warnings) ②error vs warn 심각도
#           ③데이터셋 단위 체크(is_unique·foreign_key·aggregate) ④파이프라인 독립 실행
#   ※ 중립 IoT 텔레메트리 샘플
# ============================================================================
from pyspark.sql import functions as F
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
import yaml, json

CAT = "adb_wrkspc_krc_dev"; SCH = "iot_proto"

# 1) 원천: 이미 적재된 bronze의 raw JSON을 파싱 (SDP 밖에서 at-rest 검증)
SCHEMA = ("device_id string, event_ts string, metric string, value double, "
          "unit string, battery_pct int, rssi int, seq long")
df = (spark.table(f"{CAT}.{SCH}.bronze_telemetry")
      .select(F.from_json("raw_json", SCHEMA).alias("j")).select("j.*")
      .withColumn("event_ts", F.to_timestamp("event_ts")))

# 2) DQX 체크 정의 (YAML metadata)
checks = yaml.safe_load(f"""
# ── 행 단위(error): 위반 시 quarantine 으로 분리 ──
- criticality: error
  name: device_id_not_null
  check: {{function: is_not_null, arguments: {{column: device_id}}}}
- criticality: error
  name: value_in_metric_range
  check:
    function: sql_expression
    arguments:
      expression: "(metric='temperature' AND value BETWEEN -50 AND 100) OR (metric='humidity' AND value BETWEEN 0 AND 100) OR (metric='co2' AND value BETWEEN 300 AND 5000) OR (metric='vibration' AND value BETWEEN 0 AND 100)"
      msg: value out of physical range for metric
      name: value_in_metric_range
# ── 행 단위(warn): 유지하되 _warnings 로 표시 (SDP drop 과 대비) ──
- criticality: warn
  name: battery_healthy
  check: {{function: is_in_range, arguments: {{column: battery_pct, min_limit: 0, max_limit: 100}}}}
- criticality: warn
  name: signal_ok
  check: {{function: is_not_less_than, arguments: {{column: rssi, limit: -100}}}}
- criticality: warn
  name: event_ts_not_in_future
  check: {{function: is_not_in_future, arguments: {{column: event_ts, offset: 0}}}}
# ── 데이터셋 단위(SDP expectations 로는 어려운 것) ──
- criticality: error
  name: seq_unique
  check: {{function: is_unique, arguments: {{columns: [seq]}}}}
- criticality: error
  name: device_in_reference           # 참조무결성: device_id 가 Cosmos 참조에 존재?
  check:
    function: foreign_key
    arguments:
      columns: [device_id]
      ref_columns: [device_id]
      ref_table: {CAT}.{SCH}.device_reference
- criticality: error
  name: rowcount_guard                 # 집계 가드(볼륨 이상 감지)
  check: {{function: is_aggr_not_greater_than, arguments: {{column: "*", aggr_type: count, limit: 100000}}}}
""")

dq = DQEngine(WorkspaceClient())
status = dq.validate_checks(checks)
print("checks valid:", not status.has_errors)
if status.has_errors:
    print("VALIDATION ERRORS:", status.errors)

# 3) 적용(annotate) — 모든 행에 _errors/_warnings 부착 (split 대신: warn 정보를 valid 에도 보존)
annotated = dq.apply_checks_by_metadata(df, checks)
acols = annotated.columns
ecol = "_errors"   if "_errors"   in acols else ("_error"   if "_error"   in acols else acols[-2])
wcol = "_warnings" if "_warnings" in acols else ("_warning" if "_warning" in acols else acols[-1])
etype = annotated.schema[ecol].dataType.simpleString()

has_err  = F.expr(f"{ecol} IS NOT NULL AND size({ecol}) > 0")   # size(null)=-1 → 안전
has_warn = F.expr(f"{wcol} IS NOT NULL AND size({wcol}) > 0")

# 전체(진단포함) + error행(quarantine) + valid(=error 없음, warn 은 남김)
annotated.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{CAT}.{SCH}.dqx_annotated")
annotated.filter(~has_err).write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{CAT}.{SCH}.dqx_valid")
annotated.filter(has_err).write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{CAT}.{SCH}.dqx_quarantine")

total = df.count()
v = annotated.filter(~has_err).count()
q = annotated.filter(has_err).count()
w = annotated.filter(has_warn & ~has_err).count()   # valid 이면서 경고 달린 행수
print(f"total={total} valid={v} quarantine={q} warned_valid={w}")

dbutils.notebook.exit(json.dumps({"total": total, "valid": v, "quarantine": q,
                                  "warned_valid": w, "ecol": ecol, "wcol": wcol,
                                  "etype": etype[:400]}))
