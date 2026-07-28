# 파이프라인 소스 · 설정

Lakeflow SDP(Declarative Pipelines) 프로토타입의 실제 정의 파일.

| 파일 | 설명 |
|---|---|
| `sdp_dynamic_pipeline.py` | Databricks 노트북 — YAML을 읽어 테이블을 **동적 생성**하는 SDP 파이프라인 정의 (bronze→silver→gold, 증분·CDC·데이터품질·장애 시나리오 포함) |
| `pipelines.yaml` | 파이프라인 구동 **설정** — 소스·gold 테이블 목록·품질 규칙. 이 파일만 바꾸면 테이블이 늘거나 바뀜 |
| `pipeline_config.json` | 파이프라인 **스펙** — serverless, 카탈로그/스키마, event_log(모니터링), 실패 알림, 파라미터 |

## 배포 (요약)
```bash
# 1) 설정 YAML → UC Volume
databricks fs cp ./pipelines.yaml \
  dbfs:/Volumes/adb_wrkspc_krc_dev/sdp_poc_demo/cfg/pipelines.yaml -p DEFAULT --overwrite
# 2) 노트북 → 워크스페이스 import
databricks workspace import /Users/kschoi@cloocus.com/sdp_poc_demo/sdp_dynamic_pipeline \
  --file ./sdp_dynamic_pipeline.py --language PYTHON --format SOURCE --overwrite -p DEFAULT
# 3) 파이프라인 생성 & 실행
databricks pipelines create --json @pipeline_config.json -p DEFAULT
databricks pipelines start-update <pipeline_id> -p DEFAULT
```
상세 재현은 상위 폴더 `테스트가이드.md` 참고.
