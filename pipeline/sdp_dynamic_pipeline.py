# Databricks notebook source
# ════════════════════════════════════════════════════════════════════
#  Lakeflow SDP — YAML 기반 동적 파이프라인 (확장판 v3)
#
#  ① 동적 생성      : YAML gold_tables 개수만큼 테이블 for 루프 생성
#  ② 데이터 품질    : YAML expectations 위반 행 silver에서 자동 드롭 (Dataplex DQ 대체)
#  ③ 동적 확장      : YAML 한 줄 추가 → 테이블 자동 증가
#  ④ 다층 의존성    : bronze→silver→gold 순서 자동 계산
#  ⑤/⑥ 스트리밍/증분 : landing에서 STREAMING TABLE, 재실행 시 신규분만 처리 (갭②)
#  ⑦ CDC/SCD        : AUTO CDC(apply_changes)로 변경분 병합 (SCD1 최신 / SCD2 이력)
#  ⑧ 파라미터화     : demo.min_fare 파라미터로 필터 (갭②: "날짜/값=파라미터")
# ════════════════════════════════════════════════════════════════════
import dlt
import yaml

cfg = yaml.safe_load(open(spark.conf.get("demo.config_path")))
SOURCE   = cfg["source"]
BRONZE   = cfg["bronze_table"]
SILVER   = cfg["silver_table"]
LANDING  = cfg["landing_table"]
CDC_SRC  = cfg["cdc_source"]
ORDERS   = cfg["orders_source"]
EXPECT   = cfg.get("expectations", {})
GOLDS    = cfg["gold_tables"]
MIN_FARE = float(spark.conf.get("demo.min_fare", "0"))   # ⑧ 파라미터

# ── ①②④ bronze → silver(DQ) → gold ────────────────────────────────
@dlt.table(name=BRONZE, comment="원천 복사 · bronze")
def bronze():
    return spark.read.table(SOURCE)

@dlt.table(name=SILVER, comment="정제 · silver (expectations 적용)")
@dlt.expect_all_or_drop(EXPECT)
def silver():
    return spark.sql(f"SELECT * FROM LIVE.{BRONZE}")

GOLD_SQL = ("SELECT {dim} AS dimension_value, count(*) AS trips, "
            "round(avg(fare_amount),2) AS avg_fare, round(avg(trip_distance),2) AS avg_distance "
            "FROM LIVE.{silver} GROUP BY {dim} ORDER BY trips DESC")
def make_gold(spec):
    @dlt.table(name=spec["name"], comment=f"[YAML] dim={spec['dimension']}")
    def _g():
        return spark.sql(GOLD_SQL.format(dim=spec["dimension"], silver=SILVER))
    return _g
for spec in GOLDS:
    make_gold(spec)

# ── ⑤/⑥ 스트리밍 증분: appendable landing에서 (재실행 시 신규분만) ──
@dlt.table(name="streaming_trips", comment="증분 인제스트(landing) · 신규분만 처리")
def streaming_trips():
    return spark.readStream.table(LANDING)

# ── ⑧ 파라미터화: min_fare 이상만 (파라미터로 결과 제어) ────────────
@dlt.table(name="gold_high_fare", comment=f"파라미터 min_fare 이상만 (현재 {MIN_FARE})")
def gold_high_fare():
    return spark.sql(f"SELECT * FROM LIVE.{SILVER} WHERE fare_amount >= {MIN_FARE}")

# ── ⑨ 격리(Quarantine): DQ 위반 행을 버리지 않고 별도 테이블로 ──────
#    silver = 통과 행, quarantine = 위반 행 → 무손실(둘을 합치면 bronze)
_QUAR_COND = " OR ".join(f"NOT ({c})" for c in EXPECT.values()) or "1=0"
@dlt.table(name="quarantine_trips", comment="DQ 위반 행 격리(버리지 않고 사후검토용)")
def quarantine_trips():
    return spark.sql(f"SELECT * FROM LIVE.{BRONZE} WHERE {_QUAR_COND}")

# ── ⑩ Fail-fast: 핵심 규칙 위반 시 파이프라인 즉시 중단 ─────────────
@dlt.table(name="orders_validated", comment="핵심 규칙(amount>=0) 위반 시 파이프라인 실패")
@dlt.expect_or_fail("amount_non_negative", "amount >= 0")
def orders_validated():
    return spark.read.table(ORDERS)

# ── ⑪ 부분 실패 격리 / 자동 재시도 데모 ─────────────────────────────
#    두 독립 브랜치: ok(항상 성공) / bad(런타임 장애, demo.inject_fault로 토글)
#    · 개발모드: bad만 실패, ok+나머지는 완료  → 부분 실패 격리
#    · 프로덕션모드: bad flow가 자동 재시도됨   → 자동 재시도
@dlt.table(name="branch_independent_ok",
           comment="독립 정상 브랜치 — 다른 flow 실패와 무관하게 완료(격리)")
def branch_independent_ok():
    return spark.range(100).selectExpr("id", "'ok' AS status")

_FAULT = spark.conf.get("demo.inject_fault", "true")
@dlt.table(name="branch_independent_bad",
           comment="독립 장애 브랜치 — 런타임 실패(부분실패/재시도 시연)")
def branch_independent_bad():
    # 유효 스키마(bigint) + 런타임 실패: assert_true(amount<0)는 행이 있을 때만 터짐
    # → 분석(빈 스키마 추론) 통과, 실행 시 실패 → 다른 독립 flow는 완료·커밋(부분 실패 격리)
    if _FAULT == "true":
        return spark.read.table(ORDERS).selectExpr(
            "order_id",
            "order_id + cast(assert_true(amount < 0, 'forced runtime fault') AS bigint) AS bad_calc")
    return spark.read.table(ORDERS).selectExpr("order_id", "'recovered' AS status")

# ── ⑦ CDC/SCD: AUTO CDC (apply_changes) ────────────────────────────
@dlt.view
def customer_cdc_stream():
    return spark.readStream.table(CDC_SRC)

# SCD Type 1 — 키별 "최신 상태"만 유지
dlt.create_streaming_table("customer_scd1", comment="CDC → SCD Type1 (최신 상태)")
dlt.apply_changes(
    target="customer_scd1", source="customer_cdc_stream",
    keys=["id"], sequence_by="seq",
    apply_as_deletes="op = 'DELETE'",
    except_column_list=["op", "seq"],
    stored_as_scd_type=1,
)

# SCD Type 2 — 변경 이력 보존(__START_AT/__END_AT)
dlt.create_streaming_table("customer_scd2", comment="CDC → SCD Type2 (이력 보존)")
dlt.apply_changes(
    target="customer_scd2", source="customer_cdc_stream",
    keys=["id"], sequence_by="seq",
    apply_as_deletes="op = 'DELETE'",
    except_column_list=["op", "seq"],
    stored_as_scd_type=2,
)
