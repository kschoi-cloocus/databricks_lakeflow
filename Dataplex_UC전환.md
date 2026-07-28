# Dataplex → Unity Catalog 전환 가이드 (데이터 거버넌스)

> **요건**: 기존 **Dataplex API 기반**의 메타데이터·태그·계보(lineage)·데이터 품질 처리 로직을 분석하고, **Unity Catalog + Databricks API** 기반으로 전환하는 상세 가이드 및 컨설팅.
>
> **작성일**: 2026-07-28 · GCP BigQuery → Azure Databricks 이관(과제 A: 거버넌스)
>
> 본 문서는 **개념 → 매핑 → 전환** 순서로, Dataplex를 처음 접하는 사람도 이해할 수 있게 구성했다.

---

## 목차

- [1. 큰 그림 — 한 장으로 이해하는 개념 차이](#1-큰-그림--한-장으로-이해하는-개념-차이)
  - [1.1 비유: 도서관으로 이해하기](#11-비유-도서관으로-이해하기)
  - [1.2 개념 대응 — 한눈에](#12-개념-대응--한눈에)
  - [1.3 전환의 3대 방향](#13-전환의-3대-방향)
- [2. 도메인별: 개념 → 매핑 → 전환](#2-도메인별-개념--매핑--전환)
  - [2.1 조직 (Lake / Zone)](#21-조직-lake--zone)
  - [2.2 메타데이터 (Entry)](#22-메타데이터-entry)
  - [2.3 태그 (Aspect → Governed Tags)](#23-태그-aspect--governed-tags)
  - [2.4 계보 (Lineage)](#24-계보-lineage)
  - [2.5 데이터 품질 (Data Quality Scan)](#25-데이터-품질-data-quality-scan)
- [3. API / SDK 매핑 (참고)](#3-api--sdk-매핑-참고)
- [4. 기능 갭 & 대응](#4-기능-갭--대응)
- [5. 전환 접근 (단계)](#5-전환-접근-단계)
- [6. 마이그레이션 자동화 (스크립트 골격)](#6-마이그레이션-자동화-스크립트-골격)
- [7. 체크리스트 / 다음 단계](#7-체크리스트--다음-단계)
  - [부록. 관련 문서](#부록-관련-문서)


## 1. 큰 그림 — 한 장으로 이해하는 개념 차이

### 1.1 비유: 도서관으로 이해하기
데이터(BigQuery·GCS 테이블/파일)를 **책**이라고 하자.

- **Dataplex** = 책 위에 **얹혀 있는 별도의 "도서관 관리 시스템"**.
  책(데이터)은 BigQuery에 있고, Dataplex는 그 위에서 **카드목록(색인)·라벨·출처이력·정기검수**를 따로 관리한다. → 데이터와 거버넌스가 **분리**돼 있고, 책 정보를 카드목록에 **등록/동기화**하는 작업이 계속 필요하다.

- **Unity Catalog** = **서가(데이터 플랫폼) 자체에 내장된 관리 기능**.
  책을 서가에 꽂는 순간(테이블 생성) **카드·라벨·이력·검수가 그 책에 이미 붙어 있다**. → 거버넌스가 데이터에 **내장**돼 있어, 따로 등록/동기화할 필요가 대부분 사라진다.

> **핵심 한 줄**: Dataplex는 *"데이터 위에 얹은 거버넌스 레이어"*, Unity Catalog는 *"데이터 플랫폼에 내장된 거버넌스"*. 이 차이가 전환의 방향을 결정한다.

### 1.2 개념 대응 — 한눈에

| Dataplex 개념 | 그게 뭐냐 (쉽게) | Databricks 대응 개념 |
|---|---|---|
| **Lake / Zone** | 데이터를 묶고 단계(raw/curated)별로 나누는 **구역** | **Catalog / Schema** (+ 메달리온 bronze/silver/gold) |
| **Entry / Entry Group** | 데이터 자산을 가리키는 **카드목록(색인 카드)** | **테이블/뷰/볼륨 그 자체** (별도 카드 불필요) |
| **Aspect / Aspect Type** | 책에 붙이는 **정형 라벨 양식**(필드가 정해진 스티커) | **Governed Tags** (단순 key-value 라벨) |
| **Lineage** | 이 데이터가 **어디서 왔나**(출처·파생 이력) | **자동 리니지** (테이블·컬럼, 자동 기록) |
| **Data Quality Scan** | 주기적으로 도는 **품질 검수원** | **SDP expectations / 제약 / Lakehouse Monitoring** |

### 1.3 전환의 3대 방향
1. **"등록" → "자동/내장"**: 카드목록에 자산을 등록·동기화하던 로직 → UC에서는 테이블 자체가 카드라 **대부분 소멸**.
2. **"얹은 도구" → "플랫폼 내장"**: 별도 거버넌스 제품 → UC가 데이터·컴퓨트·오케스트레이션을 **하나의 경계**로 통합.
3. **"따로 도는 검수" → "생산 공정에 인라인"**: 스케줄 품질 스캔 → 데이터 **만드는 순간** 검사(expectations) + 상시 모니터링.

> 결론: 이 작업은 순수 "이관(옮기기)"보다 **"재설계 + 매핑"** 에 가깝다. 옮길 게 줄어드는 대신, 개념을 UC 방식으로 다시 얹는다.

---

## 2. 도메인별: 개념 → 매핑 → 전환

각 도메인을 「**① 개념 이해** → **② Databricks 대응** → **③ 전환 방식** → **④ 코드**」 순으로.

### 2.1 조직 (Lake / Zone)

**① 개념** — Dataplex의 **Lake**는 여러 프로젝트의 데이터를 하나로 묶는 논리적 그릇, **Zone**은 그 안에서 정제 단계(raw / curated)로 나눈 구역이다. *도서관의 "신간코너 / 정리된 서가"*.

**② Databricks 대응** — **Catalog**(최상위 그릇, 예: 도메인·환경별) + **Schema**(하위 그룹). 정제 단계(raw→curated)는 **메달리온 bronze/silver/gold** 스키마·계층으로 자연스럽게 표현.

**③ 전환** — Lake → Catalog, Zone → Schema(또는 메달리온 계층). "raw/curated" 구역 개념이 곧 bronze/silver/gold.

**매핑**

| Dataplex | Databricks | 비고 |
|---|---|---|
| Lake | Catalog | 최상위 그릇 |
| Zone (raw) | Schema `bronze` (또는 태그 `zone=raw`) | 정제 단계 |
| Zone (curated) | Schema `silver` / `gold` | |
| Asset (BQ dataset·GCS) | Schema · Table · Volume | |

```sql
CREATE CATALOG IF NOT EXISTS health;            -- Lake
CREATE SCHEMA  IF NOT EXISTS health.bronze;     -- raw zone
CREATE SCHEMA  IF NOT EXISTS health.silver;     -- curated zone
ALTER  SCHEMA  health.silver SET TAGS ('zone'='curated');
```

---

### 2.2 메타데이터 (Entry)

**① 개념** — Dataplex **Entry**는 데이터 자산(테이블·파일)을 가리키는 **색인 카드**다. 책 자체가 아니라 "이 책은 여기 있고 이런 내용" 이라는 카드. 그래서 BigQuery 테이블이 생기면 Dataplex 카드목록에 **등록/동기화**하는 작업이 필요하다.

**② Databricks 대응** — UC에는 **별도 카드가 없다**. 테이블을 만들면 **그 테이블 정의 자체가 카드**다(스키마·설명·위치가 내장). `information_schema`가 카드목록 역할.

**③ 전환** — Entry "등록/동기화" 로직은 **대부분 제거**된다. 남는 건 설명·속성 이관뿐.

**매핑**

| Dataplex | Databricks | 비고 |
|---|---|---|
| Entry | 테이블/뷰/볼륨 **자체** | 별도 등록 불필요 |
| Entry Group | Catalog · Schema | |
| Entry Type | 자산 유형(table/view/volume/model) | 네이티브 |
| Entry 설명(description) | `COMMENT ON TABLE/COLUMN` | |
| Entry 커스텀 속성(비즈니스 메타) | **Tags** (기본) | 거버넌스 메타는 태그로 (조회·정책·검색) |
| Entry 복합/중첩 속성 | `TBLPROPERTIES` JSON (폴백) | 태그로 안 풀리는 구조만 |
| Entry 목록 조회 | `system.information_schema.tables/columns` | |

> ℹ️ `delta.*` 등 **플랫폼/엔진 설정(TBLPROPERTIES)** 은 Dataplex에 없던 개념 → **거버넌스 이관 대상 아님**. 데이터 이관 트랙에서 Databricks 베스트프랙티스로 **신규 설정**한다.

```sql
-- 설명(카드의 내용)
COMMENT ON TABLE  health.silver.vitals IS '활력징후 정제 테이블';
COMMENT ON COLUMN health.silver.vitals.patient_id IS '환자 식별자(PHI)';
-- 커스텀 속성
ALTER TABLE health.silver.vitals SET TBLPROPERTIES ('source_system'='bigquery');
```
```python
# 대량 처리는 SDK
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.tables.update(full_name="health.silver.vitals", comment="활력징후 정제 테이블")
```

> **개념 전환 요점**: "카드를 따로 만들어 동기화" → "테이블 = 카드". 동기화 파이프라인이 사라진다.

---

### 2.3 태그 (Aspect → Governed Tags)

**① 개념** — Dataplex **Aspect Type**은 **정형 라벨 양식**이다. 예: "PII정보"라는 양식에 `is_pii(불리언)·category(열거형)·owner(문자열)` 필드가 정해져 있고, 이 양식을 채워 붙인 게 **Aspect**. *필드가 정해진 스티커*. 타입이 있고 중첩도 된다.

**② Databricks 대응** — UC **Tags**는 **단순 key-value 라벨**이다. `pii=true`, `category=PHI`, `owner=hong`. 대신 **Tag Policy**로 허용 키·값을 통제(ABAC)하고, **PHI 태그로 자동 마스킹·행필터**를 건다.

**③ 전환** — 정형 양식(폼) → **평탄화된 key-value**. 중첩 필드 `pii.category` → 태그 `pii.category=PHI`. 정보는 보존하되 "폼" 구조는 평탄화. 아주 복잡한 구조만 `TBLPROPERTIES`에 JSON으로 보존.

**매핑**

| Dataplex | Databricks | 비고 |
|---|---|---|
| Aspect Type (템플릿) | Tag Policy (허용 키/값) | 스키마 검증 역할 |
| Aspect (부착 인스턴스) | Tags (key-value) | |
| Aspect 중첩 필드 | tag key `aspect.field` | 평탄화 |
| Aspect 복합/typed 구조 | `TBLPROPERTIES` JSON | 구조 보존 |
| 컬럼 aspect | 컬럼 태그 | |
| 태그 조회 | `information_schema.*_tags` | |

```sql
ALTER TABLE  health.silver.vitals SET TAGS ('data_domain'='health','pii'='true');
ALTER TABLE  health.silver.vitals ALTER COLUMN patient_id SET TAGS ('classification'='PHI');
-- 거버넌스 리포트
SELECT * FROM system.information_schema.column_tags WHERE tag_name='classification';
```

> **개념 전환 요점**: "타입 있는 양식" → "라벨(key=value)". 라벨이 곧 **접근제어 트리거**(PHI 태그 → 마스킹).

---

### 2.4 계보 (Lineage)

**① 개념** — Dataplex **Data Lineage**는 "이 데이터가 어디서 왔나"를 **process → run → lineage event**로 기록한다. BigQuery는 자동 수집되지만, 다른 처리는 **API로 직접 등록**해야 한다. *책의 출처/인용 이력*.

**② Databricks 대응** — UC는 Databricks 컴퓨트로 실행된 모든 읽기/쓰기의 **테이블·컬럼 리니지를 자동 기록**한다. 등록 코드가 거의 필요 없다. `system.access.table_lineage / column_lineage`로 조회.

**③ 전환** — **수동 리니지 등록 로직 → 대부분 삭제**(자동 수집). Databricks 밖에서 실행되는 것만 External Lineage로 보완. 과거 이력은 Dataplex export로 보관하고, UC는 전환 시점부터 새로 축적.

**매핑**

| Dataplex | Databricks | 비고 |
|---|---|---|
| Lineage event (수동 등록) | 자동 리니지 | 등록 코드 소멸 |
| BQ 자동 리니지 | UC 자동 리니지 (테이블·컬럼) | |
| Process / Run | 쿼리·잡 단위 자동 캡처 | |
| Lineage 조회 | `system.access.table_lineage` / `column_lineage`, REST | |
| 외부 시스템 리니지 | External Lineage API | 수동 등록 |

```sql
SELECT source_table_full_name, target_table_full_name, event_time
FROM system.access.table_lineage
WHERE target_table_full_name = 'health.gold.vitals_daily'
ORDER BY event_time DESC;
```

> **개념 전환 요점**: "리니지를 API로 등록/수집" → "실행하면 자동 기록". 계보 코드가 사라진다.

---

### 2.5 데이터 품질 (Data Quality Scan)

**① 개념** — Dataplex **DataScan**은 rule 기반 품질 검사(not-null·range·unique·regex 등)를 테이블에 대해 **스케줄로 따로 돌린다**. Profiling scan은 통계 프로파일. *주기적으로 도는 검수원*.

**② Databricks 대응** — 품질을 **3곳**에 배치한다:
1. **파이프라인 인라인** — SDP **expectations**(데이터 만들 때 바로 검사, 위반 시 drop/fail/warn)
2. **스키마 강제** — Delta **CHECK / NOT NULL constraint**
3. **상시 모니터링** — **Lakehouse Monitoring**(프로파일·드리프트·커스텀 지표)

**③ 전환** — "따로 도는 스캔 잡" → 데이터 **생산 공정 안으로 인라인**(품질이 파이프라인의 일부). 기존/외부 테이블만 Monitoring으로 상시 감시.

**매핑 (개념)**

| Dataplex | Databricks | 비고 |
|---|---|---|
| DataScan (DQ rule) | SDP **expectations** / Delta **constraint** | 파이프라인 인라인 |
| Data Profiling Scan | **Lakehouse Monitoring** (profile) | |
| Scan 스케줄 | 파이프라인 실행 / Jobs 스케줄 | |
| Scan 결과 저장 | `event_log`(expectation) / `*_profile_metrics` | |

**매핑 (DQ Rule 유형별)**
| Dataplex Rule | Databricks |
|---|---|
| NonNull | `@dlt.expect("nn","c IS NOT NULL")` 또는 `SET NOT NULL` |
| Range | `@dlt.expect("rng","c BETWEEN 0 AND 100")` 또는 CHECK |
| Regex / Set | `@dlt.expect(...,"c RLIKE '…'" / "c IN (…)")` |
| Uniqueness | 집계 expectation / Monitoring (Delta는 UNIQUE 하드제약 없음) |
| SQL/통계 조건 | expectation(SQL) / **Monitoring + SQL alert** |

```python
@dlt.table(name="vitals_silver")
@dlt.expect_all_or_drop({
    "valid_hr": "heart_rate BETWEEN 20 AND 300",
    "patient_present": "patient_id IS NOT NULL",
})
def vitals_silver():
    return spark.readStream.table("health.bronze.vitals")
```

> **개념 전환 요점**: "따로 돌리는 품질 스캔" → "만드는 순간 검사(expectations)" + "상시 모니터링". 품질이 별도 잡이 아니라 **파이프라인의 일부**.

---

## 3. API / SDK 매핑 (참고)

| 목적 | Dataplex (Python client) | Databricks |
|---|---|---|
| 메타 CRUD | `dataplex_v1.CatalogServiceClient` | `databricks-sdk` `w.catalogs/schemas/tables`, REST `/api/2.1/unity-catalog/*`, SQL |
| 태그/aspect | `CatalogServiceClient`(aspect) | SQL `SET TAGS`, `information_schema.*_tags` |
| 계보 | `datacatalog_lineage_v1.LineageClient` | `system.access.*_lineage`, REST `/api/2.0/lineage-tracking/*` |
| 데이터 품질 | `dataplex_v1.DataScanServiceClient` | SDP expectations, `w.quality_monitors`, Delta constraint |
| 권한 | GCP IAM | UC `GRANT`, `w.grants`, Entra ID(AAD) SCIM |

---

## 4. 기능 갭 & 대응

| # | 갭 (개념 차이에서 옴) | 대응 |
|---|---|---|
| 1 | Aspect **정형 양식(타입·중첩)** → 태그는 flat | 필드 평탄화(`aspect.field=value`) + 복합구조는 TBLPROPERTIES(JSON) |
| 2 | **외부 시스템 리니지** 자동수집 안 됨 | External Lineage 등록 / 처리를 Databricks(Lakeflow)로 흡수 |
| 3 | **과거 리니지** 이관 불가 | Dataplex export 보관, 전환 후 자동 신규 축적 |
| 4 | **Uniqueness/PK 하드 강제** 부재 | expectation/monitoring으로 검출(강제 아님) 설계 |
| 5 | **스케줄 DQ 스캔** 개념 | 파이프라인 인라인 or Jobs+Monitoring 스케줄 |
| 6 | GCP IAM → **Entra ID + UC GRANT** | 그룹·역할 매핑 재설계(SCIM) |

---

## 5. 전환 접근 (단계)

| Phase | 내용 | 산출물 |
|---|---|---|
| **P1. 인벤토리 & 매핑** | Entry/Aspect Type/DQ Rule/Lineage 소스 목록화, UC 매핑표 | 자산 인벤토리·매핑 정의서 |
| **P2. 메타·태그 이관** | Dataplex export → 변환 → UC comments/tags/properties 적용 | 이관 스크립트·태그 정책 |
| **P3. 계보 전환** | UC 자동 리니지 활성화·검증, 외부만 수동 등록 | 리니지 검증 리포트 |
| **P4. 품질 전환** | DQ Rule → SDP expectations / constraint / Monitoring | 품질 코드·Monitoring 설정 |
| **P5. 검증·병행·컷오버** | Dataplex ↔ UC 병행 → 정합성 대사 → 컷오버 | 대사 결과·컷오버 체크리스트 |

---

## 6. 마이그레이션 자동화 (스크립트 골격)

Dataplex에서 메타·태그를 읽어 UC로 적용하는 이관기 개념 골격. (실제 Aspect Type 스키마 확보 후 구체화)

```python
# migrate_governance.py — 개념 골격
from google.cloud import dataplex_v1
from databricks.sdk import WorkspaceClient

dp = dataplex_v1.CatalogServiceClient()
w  = WorkspaceClient(); WAREHOUSE_ID = "<warehouse_id>"
def run_sql(s): return w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=s)

def flatten(d, p=""):                      # 정형 aspect → 평탄 key-value
    out={}
    for k,v in (d or {}).items():
        nk=f"{p}{k}"
        out.update(flatten(v, nk+".") if isinstance(v,dict) else {nk:v})
    return out
def sane(s): return str(s).replace("'","").replace("\n"," ")[:255]

def migrate(entry, uc):                    # entry(Dataplex) → uc(예: health.silver.vitals)
    if entry.description:
        run_sql(f"COMMENT ON TABLE {uc} IS '{sane(entry.description)}'")
    for akey, aspect in (entry.aspects or {}).items():          # aspect → tags
        for f, v in flatten(aspect.data).items():
            run_sql(f"ALTER TABLE {uc} SET TAGS ('{sane(akey+'.'+f)}'='{sane(v)}')")
# 매핑표(mapping.csv: dataplex_entry, uc_full_name)를 돌며 migrate() 반복
```

---

## 7. 체크리스트 / 다음 단계

**고객 확보 필요 (실행 코드 산출 전제)**
- [ ] Dataplex **Aspect Type 정의** (필드·타입·중첩 구조)
- [ ] **Entry 규모**(테이블/파일 수) 및 UC 매핑 규칙
- [ ] **DQ Rule 목록**(유형·대상·임계치·스케줄)
- [ ] **Lineage 소스 범위**(BQ 내부 vs 외부 시스템 비중)
- [ ] **IAM 그룹·역할** ↔ Entra ID·UC 권한 매핑
- [ ] 건강데이터 **PHI 분류·마스킹 정책** 현황

**즉시 착수 가능 (자산 무관)**
- [ ] UC 메타스토어·카탈로그·스키마 설계 + Entra ID SCIM
- [ ] 태그 정책(허용 키/값) + PHI 마스킹·행필터 표준
- [ ] system 스키마 활성화 → 리니지 검증 환경
- [ ] 품질 규칙 표준 라이브러리(SDP expectations 템플릿)

---

### 부록. 관련 문서
- **확장(전환을 넘어)**: `Dataplex_UC확장.md` — UC 기반 ABAC·모니터링·페더레이션·공유·AI 거버넌스
- 데이터 파이프라인 이관 프로토타입 보고서 (`데이터파이프라인_이관_프로토타입_보고서.md`) — SDP expectations·태그·격리 실증
- 구글시트 `컨설팅_…` — 3안 비교표 / 이관난이도 / 프로토타입_결과
