# Spark 학습/프로젝트 워크스페이스

이 프로젝트는 Apache Spark 학습과 실전 프로젝트 개발을 위한 멀티 에이전트 환경입니다.

## 에이전트 구성과 호출 규칙

| 에이전트 | 언제 호출 |
|---|---|
| `spark-env-builder` | Spark 설치/Docker/클러스터 셋업 요청 |
| `spark-tutor` | "스파크 가르쳐줘", "RDD가 뭐야", "이 코드 왜 느려?" 같은 학습 질문 |
| `spark-project-planner` | "X 프로젝트 만들고 싶어" → 계획 단계 |
| `spark-project-coder` | `PROJECT_PLAN.md`의 태스크 구현 요청 |
| `spark-reviewer` | 코드 작성 직후 자동 호출 / "검토해줘" |
| `spark-data-generator` | "테스트 데이터 만들어줘", "스큐 데이터 필요해" |

## 표준 워크플로우

### 학습 모드
```
사용자 → spark-tutor → (필요시 spark-data-generator로 예제 데이터)
```

### 프로젝트 모드
```
1. spark-env-builder    : 환경 셋업
2. spark-project-planner: PROJECT_PLAN.md 작성
3. spark-data-generator : 샘플 데이터 준비
4. spark-project-coder  : 태스크별 구현
5. spark-reviewer       : 검토 → 이슈 발견 시 다시 4로
```

## 디렉토리 컨벤션
```
.
├── .claude/agents/        # 에이전트 정의
├── data_gen/              # 데이터 생성 스크립트
├── data/                  # 생성된 데이터셋
├── jobs/                  # Spark job entrypoints
├── transforms/            # 순수 transformation 함수
├── schemas/               # 스키마 정의
├── tests/                 # pytest 테스트
├── examples/              # 학습용 예제 (spark-tutor가 만듦)
└── PROJECT_PLAN.md        # 현재 프로젝트 계획
```
