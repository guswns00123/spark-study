# Spark 학습 저장소

Apache Spark를 Docker 기반 로컬 클러스터 환경에서 직접 실행하며 배우는 학습 저장소입니다.  
멀티 에이전트(Claude Code)가 레슨 설계, 코드 작성, 환경 관리, 데이터 생성을 분담하는 구조로 운영됩니다.

---

## 프로젝트 소개

| 항목 | 내용 |
|---|---|
| Spark 버전 | 3.5 (bitnamilegacy/spark:3.5) |
| 클러스터 구성 | master 1대 + worker 2대 + History Server + JupyterLab |
| 실행 환경 | Windows 10, Docker Desktop (WSL2 백엔드) |
| 학습 언어 | PySpark |
| 현재 진행 레슨 | **Lesson 03** |

이 저장소를 만든 이유:
- 개념만 읽는 학습이 아닌, 실제 분산 클러스터에서 코드를 실행하면서 배운다
- Spark UI(4040 / 18080)에서 DAG, Stage, Task를 눈으로 확인한다
- 레슨마다 demo + practice 파일을 남겨 복습 기준점으로 사용한다

---

## 환경 셋업

### 사전 요구사항

- Docker Desktop for Windows (WSL2 백엔드 권장)
- Docker Desktop > Settings > Resources > Memory: **최소 6GB** 할당
  - master 1GB + worker1 2GB + worker2 2GB + Jupyter 1GB
- PowerShell 5 이상

### 클러스터 시작 / 중지

```powershell
# 프로젝트 디렉토리로 이동
cd "C:\spark-study"

# 클러스터 시작 (백그라운드, 이미지 자동 다운로드)
docker compose up -d

# 클러스터 중지 (컨테이너 유지)
docker compose stop

# 클러스터 중지 + 컨테이너 삭제 (named volume의 노트북은 유지)
docker compose down

# 특정 서비스만 재시작
docker compose restart spark-worker-1
```

### 접속 URL

| 서비스 | URL | 설명 |
|---|---|---|
| Spark Web UI | http://localhost:8080 | 클러스터 상태, worker 목록 |
| Spark App UI | http://localhost:4040 | 실행 중인 job의 DAG / Stage / Task |
| History Server | http://localhost:18080 | 종료된 job 이력 분석 |
| JupyterLab | http://localhost:8888 | PySpark 노트북 |

Jupyter 토큰 확인:
```powershell
docker logs spark-jupyter 2>&1 | Select-String "token="
```

### 헬스체크 실행

클러스터 시작 후 20~30초 대기 후 실행합니다.

```powershell
docker exec spark-master spark-submit `
    --master spark://spark-master:7077 `
    /opt/bitnami/spark/jobs/verify-cluster.py
```

정상 출력 예시:
```
[PASS] SparkPi 분산 계산 성공
[PASS] DataFrame groupBy/agg 성공
[PASS] 파티션 분산 확인 성공
[PASS] 파일 I/O 성공
```

### Worker 스케일링

영구적으로 worker를 추가하려면 `docker-compose.yml`의 `spark-worker-2` 블록을 복사해  
`spark-worker-3`으로 이름을 바꾸세요.

---

## 학습 진행도

<!-- spark-tutor가 레슨 종료 시 자동으로 여기에 항목을 추가합니다 -->

**현재 진행 상태**: Lesson 04 완료

---

### Lesson 04: Lazy Evaluation 심화 & DAG 시각화 (2026-05-21)

- **학습 내용**: Catalyst Optimizer 가 plan 을 모아서 한 번에 최적화하는 원리를 이해하고, explain() 으로 Physical Plan 을 읽으며, Parquet PushedFilters/ReadSchema 로 최적화 증거를 체감한다
- **핵심 개념**: Lazy evaluation, Catalyst Optimizer, explain(), Filter Pushdown, Projection Pruning, PushedFilters, ReadSchema, WHERE vs HAVING, Spark UI SQL 탭
- **파일**:
  - [demo](examples/lesson_04_demo.py)
  - [practice](examples/lesson_04_practice.py)
- **다음 레슨 미리보기**: Partitioning, shuffle, narrow vs wide — 파티션 수와 데이터 분포가 성능에 미치는 영향

---

### Lesson 03: groupBy / agg / Narrow vs Wide / Shuffle (2026-05-21)

- **학습 내용**: groupBy + agg 로 여러 행을 요약하는 집계 패턴을 익히고, narrow와 wide transformation의 차이를 통해 groupBy가 왜 shuffle을 유발하는지 이해한다. 집계 후 filter(HAVING)와 orderBy(desc) 까지 SQL 5단계(WHERE → GROUP BY → 집계 → HAVING → ORDER BY)를 DataFrame API로 연결한다
- **핵심 키워드**: groupBy, agg, count/sum/avg/min/max, alias, F.desc, HAVING, Narrow vs Wide, Shuffle, Stage 경계, Spark UI Stages 탭
- **파일**:
  - [demo](examples/lesson_03_demo.py) — 부서별 집계 시나리오로 8개 Step + Narrow/Wide 다이어그램 + Shuffle 비용 설명
  - [practice](examples/lesson_03_practice.py) — TODO 5개 + 보너스 SQL 5단계 체인 퀴즈 (전부 정답 완성됨)
- **다음 레슨 미리보기**: Lazy Evaluation 심화 — explain() 으로 logical/physical plan 읽기, Spark UI DAG 탭 분석

---

### Lesson 02: 컬럼 표현식 & 내장 함수 (2026-05-20)

- **학습 내용**: col/lit 과 Column 객체의 lazy 특성을 이해하고, select/withColumn/when/otherwise 로 파생 컬럼을 만들며, 문자열/수학/날짜 내장 함수와 NULL 처리 패턴을 익힌다
- **핵심 키워드**: col, lit, select, withColumn, alias, when/otherwise, upper/lower/trim/initcap/split, round/abs, to_date/year/datediff, coalesce, explain, Catalyst
- **파일**:
  - [demo](examples/lesson_02_demo.py) — 직원 테이블 도메인으로 10개 Step 실습 + Part 2 정답 모음
  - [practice](examples/lesson_02_practice.py) — Part 1 기초 TODO 7개 + Part 2 응용 문제 5개 (코딩 테스트/면접 스타일)
- **다음 레슨 미리보기**: groupBy / agg — 집계 함수, 윈도우 함수 기초

---

### Lesson 01: SparkSession, DataFrame 기초, Lazy Evaluation (2026-05-19)

- **학습 내용**: SparkSession을 생성하고 인메모리 데이터로 DataFrame을 만들어 기본 조작(show, count, filter)을 실행하면서 Lazy Evaluation과 DAG 실행 흐름을 이해한다
- **핵심 개념**: SparkSession, Driver/Executor, createDataFrame, Lazy Evaluation, Action vs Transformation, DAG, Spark UI
- **파일**:
  - [demo](examples/lesson_01_demo.py) — SparkSession 생성, schema 정의, show/count/filter/range 실습 + Spark UI 안내
  - [practice](examples/lesson_01_practice.py) — 상품 데이터로 TODO 8개를 직접 완성하는 연습 문제
- **다음 레슨 미리보기**: RDD 기초 — RDD 생성, map/filter/reduce 변환, DataFrame과의 성능 비교

---

## 디렉토리 구조

```
C:\spark-study\
├── .claude\
│   └── agents\               # 멀티 에이전트 정의 (spark-tutor, spark-env-builder 등)
├── conf\
│   └── spark-defaults.conf   # Spark 공통 설정 (모든 컨테이너에 마운트)
├── data\                     # 학습용 데이터 (named volume 공유, git 제외)
├── data_gen\                 # 테스트 데이터 생성 스크립트
├── examples\                 # 레슨별 demo / practice 파일 (spark-tutor 생성)
├── jobs\                     # spark-submit 실행용 job 파일
├── schemas\                  # 스키마 정의
├── tests\                    # pytest 테스트
├── transforms\               # 순수 transformation 함수
├── docker-compose.yml        # 클러스터 정의 (master + worker2 + history + jupyter)
├── verify-cluster.py         # 헬스체크 스크립트
├── CLAUDE.md                 # 에이전트 구성 및 워크플로우 정의
└── README.md                 # 이 파일
```

---

## 트러블슈팅

### 포트 충돌 (8080, 7077, 8888, 4040, 18080)

```powershell
# 점유 프로세스 확인
netstat -ano | Select-String ":8080"

# 해당 PID 종료 (예: 12345)
Stop-Process -Id 12345 -Force
```

또는 `docker-compose.yml`에서 host 포트 번호를 변경합니다 (예: `"18080:8080"`).

---

### Docker 메모리 부족 (컨테이너가 자꾸 재시작)

1. Docker Desktop > Settings > Resources > Memory를 6GB 이상으로 올립니다.
2. 그래도 부족하면 `docker-compose.yml`에서 각 worker의 `SPARK_WORKER_MEMORY`를 `1G`로 낮춥니다.

---

### 한글 경로 문제 (볼륨 마운트 실패)

Windows에서 "바탕 화면" 등 한글 경로에 프로젝트가 있으면 Docker 볼륨 마운트가 실패합니다.

```powershell
# 영문 경로로 이동 후 실행
Move-Item "C:\Users\joon0\OneDrive\바탕 화면\스파크연습" "C:\spark-study"
cd "C:\spark-study"
docker compose up -d
```

---

### Worker가 master에 등록되지 않음

master가 완전히 뜨기 전에 worker가 연결을 시도해서 실패하는 경우입니다.

```powershell
# master 로그에서 등록 메시지 확인
docker logs spark-master | Select-String "Registering worker"

# worker만 재시작
docker compose restart spark-worker-1 spark-worker-2
```

---

### History Server가 비어 있음 (job이 안 보임)

`conf/spark-defaults.conf`에 아래 두 줄이 있어야 이벤트 로그가 기록됩니다.

```
spark.eventLog.enabled  true
spark.eventLog.dir      file:///opt/bitnami/spark/spark-events
```

job 실행 후 History Server(http://localhost:18080)를 새로고침하면 나타납니다.

---

### Jupyter 토큰을 모를 때

```powershell
docker logs spark-jupyter 2>&1 | Select-String "token="
```

출력된 URL(`http://127.0.0.1:8888/lab?token=...`)을 브라우저에 그대로 붙여넣습니다.

---

### 전체 초기화

```powershell
# 컨테이너 + 네트워크 삭제 (노트북 named volume 유지)
docker compose down

# named volume까지 완전 삭제 후 재시작
docker compose down -v
docker compose up -d
```
