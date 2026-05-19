# =============================================================================
# Lesson 01 — SparkSession, 인메모리 데이터 생성, 기본 출력
# =============================================================================
#
# [이 레슨에서 배우는 것]
#   1. SparkSession이 왜 필요하고 어떻게 만드는가
#   2. Driver / Executor 역할 분담
#   3. spark.createDataFrame() / spark.range() 로 데이터 만들기
#   4. show() / printSchema() / count() 의 차이
#   5. Lazy evaluation — transformation vs action
#   6. DAG가 어떻게 만들어지는지
#   7. Spark UI(localhost:4040) 에서 무엇을 보는지
#
# =============================================================================
# [핵심 개념 1] SparkSession 이란?
# =============================================================================
#
#   ┌──────────────────────────────────────────────────────┐
#   │  당신의 Python 프로세스 (Driver)                      │
#   │  ┌────────────────┐                                  │
#   │  │  SparkSession  │  ← 클러스터와 대화하는 "출입문"   │
#   │  └───────┬────────┘                                  │
#   └──────────┼───────────────────────────────────────────┘
#              │  spark://spark-master:7077
#    ┌─────────▼──────────────────────────────────────┐
#    │  Spark Master (자원 관리자)                      │
#    │   ├── Worker 1 → Executor (실제 계산 담당)       │
#    │   └── Worker 2 → Executor (실제 계산 담당)       │
#    └────────────────────────────────────────────────┘
#
#  - Driver   : 프로그램의 main() 을 실행. DAG 계획을 짜고, 태스크를 나눠 보냄.
#  - Executor : Worker 노드에서 실제로 데이터를 처리. 결과를 Driver에 돌려줌.
#  - SparkSession : 이 모든 연결과 설정을 감싸는 단일 진입점.
#                   2.0 이전의 SparkContext / HiveContext 를 하나로 통합한 것.
#
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# =============================================================================
# Step 1: SparkSession 생성
# =============================================================================
#
# builder 패턴으로 설정을 쌓은 뒤 getOrCreate() 로 세션을 얻는다.
# getOrCreate() — 이미 같은 JVM 안에 세션이 있으면 재사용, 없으면 새로 만든다.
# 클러스터 모드에서는 master URL을 따로 지정하지 않아도 된다.
# spark-submit 이 --master 옵션으로 주입해 주기 때문이다.
#
spark = (
    SparkSession.builder
    .appName("Lesson01_Demo")   # Spark UI 에서 보이는 애플리케이션 이름
    .getOrCreate()
)

# 불필요한 INFO 로그를 줄여서 출력이 깔끔하게 보이도록 설정
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("SparkSession 생성 완료")
print(f"  App Name : {spark.sparkContext.appName}")
print(f"  Master   : {spark.sparkContext.master}")
print(f"  Version  : {spark.version}")
print("=" * 60)

# =============================================================================
# Step 2: 인메모리 데이터 만들기 (방법 A — createDataFrame)
# =============================================================================
#
# [핵심 개념 2] DataFrame 이란?
#   - 이름 붙은 컬럼과 타입 정보를 가진 분산 테이블.
#   - 내부는 RDD 위에 스키마를 얹은 것.
#   - Pandas DataFrame 처럼 생겼지만, 실제 데이터는 여러 Executor 에 분산됨.
#
# 스키마를 명시하면 Spark 이 타입 추론을 생략하므로 약간 더 빠르다.
#
schema = StructType([
    StructField("name",   StringType(),  nullable=False),  # 직원 이름
    StructField("dept",   StringType(),  nullable=False),  # 부서
    StructField("salary", IntegerType(), nullable=False),  # 연봉 (만원)
    StructField("score",  DoubleType(),  nullable=True),   # 평가 점수 (없을 수도 있음)
])

# Python 리스트 → Spark DataFrame
# 이 시점에서 데이터는 아직 Executor 로 전송되지 않는다. (Lazy evaluation!)
employees = spark.createDataFrame(
    data=[
        ("김민준", "엔지니어링", 5500, 4.5),
        ("이서연", "마케팅",     4800, 3.9),
        ("박지훈", "엔지니어링", 6200, 4.8),
        ("최유진", "인사",       4200, None),   # score 없음 → None = null
        ("정하은", "마케팅",     5100, 4.1),
        ("한도현", "엔지니어링", 5800, 4.6),
    ],
    schema=schema,
)

# =============================================================================
# [핵심 개념 3] Lazy Evaluation — transformation vs action
# =============================================================================
#
#  위의 createDataFrame() 호출은 아직 클러스터에서 아무 일도 하지 않았다.
#  Spark 은 "무엇을 할지" 계획(DAG)을 먼저 쌓고,
#  action 이 호출될 때 비로소 실행한다.
#
#  Transformation (계획만 추가, 실행 안 함):
#    select(), filter(), groupBy(), join(), withColumn() ...
#
#  Action (실제 실행 트리거):
#    show(), count(), collect(), write() ...
#
#  왜 Lazy 하게? → Catalyst Optimizer 가 전체 계획을 보고
#  불필요한 스캔/셔플을 제거할 수 있기 때문이다.
#
#  DAG 흐름 (이 레슨):
#
#    createDataFrame ──(transformation)──▶ filter ──(action)──▶ show
#         │                                                        │
#      계획 추가                                            실행 시작!
#         │                                                        │
#         └──────────── DAG Stage 1 ────────────────────────────-┘
#

# =============================================================================
# Step 3: printSchema() — 스키마 확인 (action)
# =============================================================================
#
# printSchema() 는 Executor 에서 데이터를 읽지 않고 메타데이터만 출력한다.
# 가장 빠른 "내 DataFrame 이 어떻게 생겼나" 확인 방법.
#
print("\n[printSchema] — 컬럼 이름, 타입, nullable 여부 출력")
print("-" * 40)
employees.printSchema()

# =============================================================================
# Step 4: show() — 데이터 미리보기 (action)
# =============================================================================
#
# show(n) 은 처음 n 행을 출력한다. 기본값 20.
# 이 순간 DAG 가 실행된다:
#   1. Driver 가 Task 를 Executor 에게 보낸다.
#   2. Executor 가 파티션 데이터를 처리한다.
#   3. 결과가 Driver 로 전송되어 콘솔에 출력된다.
#
# Spark UI(localhost:4040) → Jobs 탭에 새 Job 이 생긴다!
#
print("\n[show] — 처음 10행 출력 (action → DAG 실행 시작)")
print("-" * 40)
employees.show(10, truncate=False)
#                  truncate=False : 긴 문자열을 자르지 않고 전부 출력

# =============================================================================
# Step 5: count() — 행 수 세기 (action)
# =============================================================================
#
# count() 는 매우 흔하지만, 내부적으로 전체 파티션을 스캔하므로
# 데이터가 클 때는 비용이 크다.
# Spark UI 에서 이 Job 의 Stage 를 클릭하면 Task 수와 처리 시간을 볼 수 있다.
#
row_count = employees.count()  # action
print(f"\n[count] 전체 행 수 = {row_count}")

# =============================================================================
# Step 6: spark.range() — 숫자 시퀀스 DataFrame (방법 B)
# =============================================================================
#
# range(N) 은 0 ~ N-1 의 id 컬럼 하나짜리 DataFrame 을 만든다.
# 파티션을 직접 지정할 수 있어서 분산 동작 실험에 유용하다.
#
print("\n[range] 0~9 숫자 시퀀스, 파티션 2개로 나누기")
print("-" * 40)
numbers = spark.range(10, numPartitions=2)
numbers.show()

# 파티션 수 확인 — 파티션이 곧 병렬 Task 수
print(f"파티션 수 = {numbers.rdd.getNumPartitions()}")
#   → Spark UI Jobs → Stages 탭에서 Task 가 2개인 것을 확인할 수 있다.

# =============================================================================
# Step 7: filter() transformation + show() action 체인
# =============================================================================
#
# filter() 는 transformation 이라 아직 실행 안 됨.
# .show() 를 붙이는 순간 DAG 전체(createDataFrame → filter → show) 가 실행.
#
print("\n[filter + show] 엔지니어링 부서만 보기")
print("-" * 40)
engineers = employees.filter(employees["dept"] == "엔지니어링")
engineers.show(truncate=False)

# =============================================================================
# [핵심 개념 4] Spark UI 에서 확인할 것
# =============================================================================
#
# 브라우저에서 http://localhost:4040 을 열어보세요.
#
#  1. Jobs 탭
#     - 이 스크립트를 실행하는 동안 여러 Job 이 생긴다.
#     - 각 Job 은 하나의 action(show, count 등) 에 대응한다.
#     - "Description" 컬럼을 보면 어느 코드 줄이 트리거했는지 표시된다.
#
#  2. Stages 탭
#     - 각 Job 은 하나 이상의 Stage 로 나뉜다.
#     - Stage 안에서는 데이터가 셔플 없이 흐른다.
#     - Task 수 = 파티션 수. 각 Task 가 Executor 에서 병렬 실행된다.
#
#  3. SQL / DataFrame 탭 (있을 경우)
#     - Catalyst Optimizer 가 만든 실행 계획을 그래프로 볼 수 있다.
#     - "Parsed → Analyzed → Optimized → Physical Plan" 4단계 확인 가능.
#
#  4. Executors 탭
#     - 현재 연결된 Executor 목록, 메모리/CPU 사용량 확인.
#
# =============================================================================

print("\n" + "=" * 60)
print("Lesson 01 Demo 완료!")
print("localhost:4040 (Jobs / Stages / SQL) 을 확인해 보세요.")
print("=" * 60)

# SparkSession 종료 — 클러스터 자원을 반납한다.
spark.stop()
