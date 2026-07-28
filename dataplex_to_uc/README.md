# Dataplex → Unity Catalog 이관 실측 스크립트

전환 가이드(`../Dataplex_UC전환.md`)를 **실제 GCP + Databricks 환경에서 end-to-end 실증**한 코드.
결과 리포트: `../Dataplex_to_UC_테스트결과.md`

## 사전 준비
- **GCP**: 대상 프로젝트에 BigQuery+Dataplex 권한 SA 키 → 각 스크립트 상단 `KEY=<PATH>/google-sa.json` 수정
  - 필요 role: `bigquery.dataEditor`+`jobUser`, `dataplex.admin`, `datacatalog.admin`, `datalineage.viewer`
  - API 활성화: bigquery, dataplex, datacatalog, datalineage
- **Databricks**: CLI `DEFAULT` 프로파일 + SQL 웨어하우스 → `05/06`의 `WID`(warehouse_id)·`CAT`(catalog) 수정
- `pip install google-cloud-bigquery google-cloud-dataplex google-cloud-datacatalog-lineage`

## 실행 순서
| # | 스크립트 | 역할 |
|---|---|---|
| 1 | `01_bigquery_sample.py` | BigQuery 샘플(테이블·설명·라벨·파생) 생성 |
| 2 | `02_dataplex_datascan.py` | Dataplex DataScan(데이터 품질) 생성·실행·결과 |
| 3 | `03_dataplex_aspect.py` | AspectType + EntryGroup + Entry(+aspect) 생성 |
| 4 | `04_read_governance.py` | GCP 거버넌스 → `gov_inventory.json` |
| 5 | `05_migrate_to_uc.py` | UC 미러 테이블 + comments/tags/constraints/lineage |
| 6 | `06_verify_uc.py` | system/information_schema 검증 |

`gov_inventory.sample.json` — 04 실행 결과 예시(참고).

## 매핑 요약 (스크립트가 수행하는 이관)
| Dataplex(소스) | Unity Catalog(타깃) |
|---|---|
| BQ description | `COMMENT ON TABLE/COLUMN` |
| BQ labels | 테이블 태그 |
| Aspect(구조화) | 태그(평탄화 `aspecttype_field`) |
| DataScan 규칙 | `NOT NULL` / `CHECK` 제약 |
| BQ lineage | UC 자동 리니지(CTAS 실행 시) |

## 주의
- **Dataplex API 쿼터**: 리전당 분당 ~10요청 → 스크립트에 백오프 내장.
- **리니지 지연**: BQ·UC 모두 인덱싱에 수 분 → 즉시 검증은 UI/재조회.
- 데이터는 **합성 대표 샘플**(건강데이터 PHI 관점).
