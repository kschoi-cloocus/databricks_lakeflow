# Unity Catalog 확장 가이드 — 전환을 넘어

> **전환**(`Dataplex_UC전환.md`)이 *"Dataplex가 하던 것을 UC로 재현"* 이라면, 본 문서는 **확장** — *"UC라서 새로 할 수 있는 것"* 을 다룬다.
>
> **작성일**: 2026-07-28 · GCP BigQuery → Azure Databricks 이관(과제 A: 거버넌스) · 건강데이터(PHI) 관점

---

## 목차

- [1. 전환 vs 확장 — 왜 확장인가](#1-전환-vs-확장--왜-확장인가)
- [2. 확장 지도 (한눈에)](#2-확장-지도-한눈에)
- [3. 영역별 상세](#3-영역별-상세)
  - [3.1 거버넌스 대상 확장 — 데이터를 넘어 AI까지](#31-거버넌스-대상-확장--데이터를-넘어-ai까지)
  - [3.2 ABAC + 자동 분류·마스킹 — 정책 as code](#32-abac--자동-분류마스킹--정책-as-code)
  - [3.3 Lakehouse Monitoring — 품질에서 드리프트·모델까지](#33-lakehouse-monitoring--품질에서-드리프트모델까지)
  - [3.4 AI 기반 거버넌스 — 자동 문서화·태깅·자연어](#34-ai-기반-거버넌스--자동-문서화태깅자연어)
  - [3.5 Lakehouse Federation — 외부 소스도 UC로](#35-lakehouse-federation--외부-소스도-uc로)
  - [3.6 안전한 공유·협업 — Delta Sharing / Marketplace / Clean Rooms](#36-안전한-공유협업--delta-sharing--marketplace--clean-rooms)
  - [3.7 System Tables — 거버넌스를 "데이터"로](#37-system-tables--거버넌스를-데이터로)
  - [3.8 시맨틱·인증 계층 — 신뢰할 수 있는 데이터](#38-시맨틱인증-계층--신뢰할-수-있는-데이터)
- [4. 건강데이터(PHI) 관점 우선순위](#4-건강데이터phi-관점-우선순위)
- [5. 로드맵 — 전환 → 확장](#5-로드맵--전환--확장)
- [6. 요약](#6-요약)
  - [부록. 관련 문서](#부록-관련-문서)


## 1. 전환 vs 확장 — 왜 확장인가

- **Dataplex** = 주로 **데이터 카탈로그·거버넌스**(메타·태그·계보·품질) 중심.
- **Unity Catalog** = 데이터 + **AI/ML 모델 · 함수 · 노트북/대시보드 · 볼륨 · 데이터 공유**까지 아우르는 **레이크하우스 거버넌스 플랫폼**.

그래서 **전환만** 하면 "Dataplex 기능 재현"에서 멈추지만, UC의 통합성을 활용하면 **Dataplex엔 없거나 약했던 영역**이 열린다.

> 🏗️ **비유**: 전환이 *"같은 서비스를 새 건물로 이사"* 라면, 확장은 *"새 건물이라 비로소 가능해진 새 서비스"* 다. 이사만 하고 멈추면 새 건물의 절반만 쓰는 셈.

---

## 2. 확장 지도 (한눈에)

| 확장 영역 | Dataplex | Unity Catalog 확장 | 건강데이터(PHI) 가치 |
|---|---|---|---|
| 거버넌스 대상 | 데이터 메타 중심 | + **ML 모델·함수·볼륨·노트북·대시보드** 통합 | 모델·PHI 파일까지 **한 경계** |
| 접근제어 | 정적 태그 · IAM | **ABAC**(태그 기반 정책) · **동적 마스킹·행필터** | PHI **자동 마스킹**을 대규모로 |
| 데이터 분류 | 제한적 | **민감정보 자동 탐지·태깅** | PHI 식별 자동화 |
| 품질·모니터링 | DQ 스캔 | **Lakehouse Monitoring**(품질+드리프트+모델) | 지표 이상·모델 드리프트 감시 |
| AI 거버넌스 | — | **AI 자동 문서화·태깅**, **Genie 자연어** | 카탈로그 이해도·검색성↑ |
| 외부 소스 통합 | — | **Lakehouse Federation** | 전환기 **BigQuery 병행** 거버넌스 |
| 데이터 공유 | GCP 내부 | **Delta Sharing · Marketplace · Clean Rooms** | 연구기관·파트너 **안전 공유** |
| 관측·감사 | 부분 | **System Tables**(감사·비용·리니지) SQL | **컴플라이언스 대시보드** |
| 시맨틱 계층 | — | **Metric Views · 인증 자산 · 글로서리** | 신뢰 지표 표준화 |

---

## 3. 영역별 상세

### 3.1 거버넌스 대상 확장 — 데이터를 넘어 AI까지
Dataplex는 데이터 자산 중심. UC는 **하나의 카탈로그·권한·태그·리니지 체계로 데이터뿐 아니라**:
- **ML 모델**(UC Model Registry) — 모델도 catalog.schema.model로 거버넌스·리니지·권한
- **함수(UDF)**, **볼륨**(비정형 파일: 영상·PDF·의료 이미지), **노트북·대시보드**

> **왜 확장인가**: PHI가 테이블뿐 아니라 **모델·파일**에도 흐른다. UC는 이들을 **같은 태그·마스킹·감사 경계**에 넣는다. (예: PHI로 학습된 모델의 리니지·접근을 데이터와 동일하게 통제)

### 3.2 ABAC + 자동 분류·마스킹 — 정책 as code
정적 GRANT를 넘어 **태그(속성) 기반 정책**으로 대규모 자동 적용.
```sql
-- 컬럼 마스크 함수 + 태그 기반 적용 (권한 그룹만 원본, 나머지는 마스킹)
CREATE FUNCTION health.sec.mask_phi(v STRING)
  RETURN CASE WHEN is_account_group_member('phi_reader') THEN v ELSE '***-****' END;
ALTER TABLE health.silver.vitals ALTER COLUMN patient_id SET MASK health.sec.mask_phi;

-- 행 필터: 담당 부서 데이터만
CREATE FUNCTION health.sec.dept_filter(dept STRING)
  RETURN is_account_group_member(concat('dept_', dept));
ALTER TABLE health.silver.vitals SET ROW FILTER health.sec.dept_filter ON (department);
```
- **ABAC**: "**PHI 태그가 붙은 모든 컬럼**은 비권한 그룹에 자동 마스킹" 같은 **정책 1개를 카탈로그 전체에** 적용(수백 테이블을 개별 설정하지 않음).

> **왜 확장인가**: Dataplex는 태그를 붙이지만 **태그→접근제어 자동 연결**이 약하다. UC는 **태그가 곧 정책 트리거** → 신규 테이블이 생겨도 태그만 맞으면 자동 보호.

### 3.3 Lakehouse Monitoring — 품질에서 드리프트·모델까지
전환의 "expectations(품질)"를 넘어, 기존/외부 테이블과 **ML 모델까지 상시 감시**.
- **Profile**(분포·null·통계) · **Drift**(시간에 따른 변화) · **Inference/모델 모니터링**(예측 품질·드리프트) · **커스텀 지표**
- 자동 생성되는 `*_profile_metrics` / `*_drift_metrics` 테이블 → **SQL alert·대시보드**로 임계치 감시

> **왜 확장인가**: Dataplex DQ는 "규칙 통과/실패" 위주. UC는 **"데이터·모델이 시간에 따라 변하는지(drift)"** 까지 → 건강지표 이상 탐지·모델 성능 저하 조기 경보.

### 3.4 AI 기반 거버넌스 — 자동 문서화·태깅·자연어
- **AI 자동 문서화**: 테이블/컬럼 **설명(comment)을 LLM이 제안** → 카탈로그 문서화 비용↓
- **민감정보 자동 분류·태깅**: PHI/PII 후보 컬럼 자동 탐지 → 태그 제안
- **Genie (AI/BI)**: 거버넌스된 데이터에 **자연어 질의** ("지난달 부서별 평균 심박수") → 비개발자 셀프서비스, 단 **권한·마스킹은 그대로 적용**

> **왜 확장인가**: Dataplex엔 없던 **"거버넌스 + 생성형 AI"** 결합. 문서화·분류를 사람이 다 하던 걸 AI가 초안 → 검수만.

### 3.5 Lakehouse Federation — 외부 소스도 UC로
데이터를 옮기지 않고 **BigQuery·Snowflake·MySQL 등 외부 소스를 UC 거버넌스 안에서 쿼리**.
```sql
CREATE CONNECTION bq_src TYPE bigquery OPTIONS (/* SA 자격증명 */);
CREATE FOREIGN CATALOG bq FOREIGN CONNECTION bq_src;
SELECT * FROM bq.dataset.table;   -- BigQuery를 UC 태그·권한·리니지 아래에서
```
> **왜 확장인가**: **전환 과도기**에 아직 BigQuery에 남은 데이터를 **옮기기 전부터 UC로 통제·조회** 가능 → 점진 이관의 안전장치. 전환 후에도 타 시스템 연동에 유효.

### 3.6 안전한 공유·협업 — Delta Sharing / Marketplace / Clean Rooms
```sql
-- Delta Sharing: 복사 없이 거버넌스된 데이터 공유 (크로스 클라우드/조직)
CREATE SHARE research_share;
ALTER SHARE research_share ADD TABLE health.gold.deid_metrics;   -- 비식별 지표만
CREATE RECIPIENT partner USING ID '<recipient-id>';
GRANT SELECT ON SHARE research_share TO RECIPIENT partner;
```
- **Marketplace**: 내부/외부에 **데이터 프로덕트**로 발행·구독
- **Clean Rooms**: 원본을 서로 노출하지 않고 **프라이버시 안전 협업**(집계·조인만)

> **왜 확장인가**: 건강데이터 특성상 **연구기관·파트너와의 공유**가 잦다. Dataplex엔 없는 **개방형 공유·클린룸**으로 **PHI를 노출하지 않고** 협업(비식별·집계 기반).

### 3.7 System Tables — 거버넌스를 "데이터"로
거버넌스 활동 자체를 **SQL로 질의 가능한 시스템 테이블**로 → 감사·비용·접근 리뷰를 **대시보드화**.
```sql
-- PHI 테이블 최근 접근 감사 (컴플라이언스)
SELECT event_time, user_identity.email, action_name
FROM system.access.audit
WHERE request_params.full_name_arg LIKE 'health.%'
ORDER BY event_time DESC;
-- 비용 귀속 (system.billing.usage), 리니지 (system.access.*_lineage)
```
> **왜 확장인가**: Dataplex는 감사·비용·리니지가 분산·부분적. UC는 **모두 하나의 system 스키마**에 → **접근 리뷰·이상탐지·비용 최적화 대시보드**를 SQL로 자체 구축.

### 3.8 시맨틱·인증 계층 — 신뢰할 수 있는 데이터
- **Metric Views**: 핵심 지표(예: 재원일수·재입원율)를 **한 곳에 정의**한 시맨틱 계층 → BI마다 다른 계산 방지
- **Certified/Endorsed**: 검증된 자산 **인증 배지** → 신뢰 자산 식별
- **Business Glossary**: 업무 용어 표준화 + 자산 연결

> **왜 확장인가**: "카탈로그에 있다"를 넘어 **"믿고 쓸 수 있다(certified) + 계산이 통일됐다(metric view)"** 로. 거버넌스가 **데이터 신뢰**로 이어진다.

---

## 4. 건강데이터(PHI) 관점 우선순위

| 확장 | 우선도 | 이유 |
|---|---|---|
| ABAC + 자동 분류·마스킹 (3.2) | 🔴 최상 | PHI 대규모 자동 보호 — 규제 대응 핵심 |
| System Tables 감사 (3.7) | 🔴 최상 | 접근 감사·컴플라이언스 증빙 |
| Lakehouse Monitoring (3.3) | 🟠 상 | 건강지표 이상·모델 드리프트 감시 |
| Clean Rooms / Delta Sharing (3.6) | 🟠 상 | 연구·파트너와 PHI 비노출 협업 |
| Lakehouse Federation (3.5) | 🟡 중 | 전환 과도기 BigQuery 병행 통제 |
| AI 거버넌스 (3.4) | 🟡 중 | 문서화·분류 자동화로 운영 효율 |

---

## 5. 로드맵 — 전환 → 확장

| Wave | 초점 | 내용 |
|---|---|---|
| **Wave 1 — 전환(재현)** | Dataplex 기능 UC로 이관 | 메타·태그·계보·품질 (→ `Dataplex_UC전환.md`) |
| **Wave 2 — 기반 확장** | 자동화·통제 강화 | **ABAC·마스킹**, **System Tables 감사**, **Federation**(BigQuery 병행) |
| **Wave 3 — 고급 확장** | 신뢰·협업·AI | **Monitoring(드리프트/모델)**, **Clean Rooms/Sharing**, **AI 거버넌스**, **Metric Views/인증** |

> 전환(Wave 1)과 병행해 **Wave 2 일부(감사·마스킹)를 조기 착수**하면, 건강데이터 규제 대응을 초기부터 확보할 수 있다.

---

## 6. 요약

- **전환**은 "옮기기", **확장**은 "새로 열리는 것". UC는 단순 카탈로그가 아니라 **레이크하우스 전체 거버넌스 플랫폼**이라, Dataplex에 없던 **ABAC·자동분류·드리프트 모니터링·페더레이션·안전공유·AI 거버넌스·시맨틱 계층**이 열린다.
- 건강데이터(PHI) 관점에선 **ABAC 자동 마스킹 + System Tables 감사**가 최우선 확장.
- 이관을 "재현"에서 멈추지 말고 **확장까지 로드맵으로** 잡는 것을 권고.

---

### 부록. 관련 문서
- **전환(재현)**: `Dataplex_UC전환.md` — 메타데이터·태그·계보·품질의 1:1 전환
- 데이터 파이프라인 이관 프로토타입 보고서 (`데이터파이프라인_이관_프로토타입_보고서.md`)

> ⚠️ 일부 기능(민감정보 자동분류·ABAC·Metric Views 등)은 GA/Preview 상태가 시점에 따라 다르므로, 도입 전 최신 릴리스 상태 확인 권장.
