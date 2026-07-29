# realtime_source_proto — 실행 코드

BigQuery(배치/이력) + Event Hub·Cosmos(실시간) → Databricks SDP 프로토타입의 재현 코드입니다.
상세 설명은 상위 [`BigQuery_EventHub_Cosmos_프로토타입.md`](../BigQuery_EventHub_Cosmos_프로토타입.md) 참고.

## 자격증명 (리포에 미포함)

코드는 아래에서 자격증명을 로드하며, **평문 키를 코드/리포에 두지 않습니다.**

| 위치 | 내용 |
|---|---|
| `.secrets/eventhub.json` | Event Hub 연결문자열 (로컬) |
| `.secrets/cosmos.json` | Cosmos endpoint/key (로컬) |
| `.secrets/google-sa.json` | GCP 서비스계정 키 (로컬) |
| Databricks Secret `iot_proto/eh_conn_str` | 파이프라인 런타임용 EH 연결문자열 |

> 파일 상단의 리소스 식별자(프로젝트/네임스페이스/warehouse ID/catalog)는 예시이며 환경에 맞게 교체합니다.

## 실행 순서

| 순서 | 파일 | 설명 |
|---|---|---|
| 1 | `01_inject_cosmos.py` | Cosmos `device_state` 20대 주입 |
| 2 | `02_inject_eventhub.py` | Event Hub `device-telemetry` 600 이벤트 주입(불량 37 포함) |
| 3 | `03_cosmos_to_delta.py` | Cosmos → Delta 참조테이블(`device_reference`) 브리지 |
| 4 | `run_pipeline.py` | `04_sdp_eventhub_pipeline.py`를 업로드→파이프라인 생성→실행 |
| 5 | `verify_pipeline.py` | 메달리온 행수·품질 격리·조인·gold 검증 |
| A1 | `05_bq_sample.py` | BigQuery 샘플 warehouse 생성(2,400+20행) |
| A2 | `06_federation_setup.py` | Lakehouse Federation 연결·라이브 조회 |
| A3 | `07_bq_export_load.py` | BigQuery 배치 export → UC Volume → Delta 이관 |

`04_sdp_eventhub_pipeline.py`는 SDP 파이프라인 **본체**(Databricks 노트북 소스)로, `run_pipeline.py`가 워크스페이스에 업로드해 실행합니다.

## 의존성

```bash
pip install azure-eventhub azure-cosmos google-cloud-bigquery
# + Databricks CLI (profile 설정), az CLI(SP 로그인)
```
