# Dataplex → Unity Catalog 이관 실측 테스트 결과

> **목적**: 앞선 전환 가이드(`Dataplex_UC전환.md`)를 **실제 GCP + Databricks 환경에서 end-to-end로 실증**. 대표 샘플 거버넌스를 GCP Dataplex에 구성한 뒤 Unity Catalog로 이관하고, system/information_schema로 검증했다.
>
> **작성일**: 2026-07-28 · 건강데이터(PHI) 대표 샘플 (합성 데이터)

---

## 1. 테스트 환경

| 구분 | 값 |
|---|---|
| GCP 프로젝트 | `myproject-493803` · 리전 `asia-northeast3`(서울) |
| BigQuery | 데이터셋 `health_demo` (patients / vitals / vitals_daily) |
| Dataplex | DataScan `vitals-dq`, AspectType `data-classification`, EntryGroup `health-catalog`/Entry `patients` |
| Databricks(UC) | `adb_wrkspc_krc_dev.gov_demo` (patients / vitals / vitals_daily) |

> 실제 고객 Dataplex가 미구성이라, **대표 샘플을 GCP에 실제로 생성**해 테스트했다(합성 데이터).

---

## 2. 구성한 GCP Dataplex 거버넌스 (소스)

- **메타데이터**: BQ 테이블·컬럼 **설명(description)** + **라벨(labels)** (`data_domain=health`, `pii=true`, `layer=bronze/gold`)
- **Aspect(구조화 메타)**: AspectType `data-classification`(필드: sensitivity/pii_category/steward) → Entry `patients`에 부착 `{sensitivity:high, pii_category:PHI, steward:data-team}`
- **데이터 품질**: DataScan `vitals-dq` — 규칙 3개(patient_id NonNull, heart_rate 20~300, systolic 50~250) → **실행 결과 전체 통과, 5행 평가**
- **계보**: `vitals → vitals_daily` (BigQuery CTAS, Data Lineage 자동)

---

## 3. 이관 실측 (Dataplex 자산 → Unity Catalog)

거버넌스 인벤토리를 API로 추출(`gov_inventory.json`) → UC에 적용 → 검증.

### ① 메타데이터 → COMMENT ✅
컬럼 설명 **10건** 모두 이관 (예):
```
patients.patient_id → 환자 식별자 (PHI)
vitals.heart_rate   → 심박수 (bpm)
```

### ② 라벨 + Aspect → UC 태그 ✅
`information_schema.table_tags` 검증:
| 테이블 | 태그 |
|---|---|
| patients | `data_domain=health`, `pii=true`, `layer=bronze`, **`data_classification_pii_category=PHI`**, **`data_classification_sensitivity=high`**, **`data_classification_steward=data-team`** |
| vitals | `data_domain=health`, `pii=true`, `layer=bronze` |
| vitals_daily | `data_domain=health`, `layer=gold` |

> Aspect(구조화)는 **평탄화**되어 `data_classification_*` 태그로 이관됨(가이드의 aspect→tag 전환 실증). *태그 키에 하이픈 불가 → 언더스코어로 정규화.*

### ② 컬럼 PHI 분류 → 컬럼 태그 ✅
`information_schema.column_tags`:
```
patients.name       classification=PHI
patients.patient_id classification=PHI
vitals.patient_id   classification=PHI
```

### ④ DataScan(DQ) → Delta 제약 ✅
| 규칙(Dataplex) | UC 제약 |
|---|---|
| patient_id NonNull | `NOT NULL` (information_schema에서 is_nullable=NO 확인) |
| heart_rate 20~300 | `delta.constraints.chk_heart_rate = heart_rate BETWEEN 20 AND 300` |
| systolic 50~250 | `delta.constraints.chk_systolic = systolic BETWEEN 50 AND 250` |

> CHECK 제약은 `SHOW TBLPROPERTIES`(`delta.constraints.*`)로 확인. *일부 워크스페이스에선 `information_schema.check_constraints`에 미표출.*

### ⑤ 계보 → UC 자동 리니지 ⏳
UC에서 `CREATE TABLE vitals_daily AS SELECT … FROM vitals` 실행 → **리니지 등록 코드 없이 자동 캡처**. 단 `system.access.table_lineage` **인덱싱 지연**으로 실행 직후엔 미표출(수 분 후·UI에서 확인).

---

## 4. 검증 요약

| 도메인 | Dataplex(소스) | UC(타깃) | 검증 테이블 | 결과 |
|---|---|---|---|---|
| 메타데이터 | description | COMMENT | `information_schema.columns` | ✅ 10건 |
| 태그 | labels + aspect | table tags | `information_schema.table_tags` | ✅ 11건 |
| 분류 | PHI(컬럼) | column tags | `information_schema.column_tags` | ✅ 3건 |
| 품질 | DataScan 3규칙 | NOT NULL + CHECK×2 | `SHOW TBLPROPERTIES` | ✅ |
| 계보 | BQ lineage | UC 자동 리니지 | `system.access.table_lineage` | ⏳ 수집 지연 |

---

## 5. 재현 방법 (스크립트)

`dataplex_to_uc/` 폴더, 순서대로 실행 (SA 키 + Databricks DEFAULT 프로파일 필요):
```
01_bigquery_sample.py     # BQ 샘플(설명·라벨·파생) 생성
02_dataplex_datascan.py   # DataScan(DQ) 생성·실행
03_dataplex_aspect.py     # AspectType/EntryGroup/Entry(+aspect) 생성
04_read_governance.py     # GCP 거버넌스 → gov_inventory.json
05_migrate_to_uc.py       # UC 미러 테이블 + comments/tags/constraints/lineage
06_verify_uc.py           # system/information_schema 검증
```

---

## 6. 한계·참고

- **Dataplex API 쿼터**: DataScan 등 분당 10요청/리전 → 호출 간격·백오프 필요(스크립트 반영).
- **리니지 지연**: BigQuery·UC 모두 리니지 인덱싱에 수 분 소요. 즉시 검증엔 UI/재조회.
- **CHECK 제약 노출**: `information_schema.check_constraints` 미표출 케이스 → `SHOW TBLPROPERTIES`로 확인.
- **데이터는 합성 샘플**: 실제 이관 시 규모·복잡도에 따라 자동화 스크립트를 확장(전환 가이드 6장 골격).

---

## 7. 결론

전환 가이드의 **메타데이터·태그(라벨+구조화 aspect)·데이터 품질·계보 매핑이 실제 환경에서 동작함을 실증**했다. 특히 **Aspect(구조화) → 평탄화 태그**, **DataScan → Delta 제약**, **UC 자동 리니지**가 가이드대로 이관됨을 system/information_schema로 확인했다. 실제 프로젝트 적용은 Action Plan(`Dataplex_to_UC_ActionPlan.md`)의 Phase대로 진행한다.

### 관련 문서
- `Dataplex_UC전환.md`(개념·매핑) · `Dataplex_UC확장.md`(확장) · `Dataplex_to_UC_ActionPlan.md`(실행계획)
