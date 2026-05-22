# =============================================================================
# Lesson 04 — Lazy Evaluation 심화 & DAG 시각화
# =============================================================================
#
# [이 레슨에서 배우는 것]
#   1. Lazy evaluation 재정의 — "왜" lazy인가 (Catalyst의 global optimization)
#   2. "lazy가 아니라면" 반례로 lazy의 효용 체감
#   3. explain() 으로 Physical Plan 읽는 법 (아래에서 위로)
#   4. Filter Pushdown + Projection Pruning 을 코드로 증명
#   5. Parquet 파일에서 PushedFilters / ReadSchema 확인
#   6. Action vs Transformation — lazy 동작을 Jobs 탭에서 확인
#   7. Spark UI SQL/DataFrame 탭 + Jobs 탭 DAG 읽는 법
#
# [Lesson 01~03 과의 연결]
#   Lesson 01: lazy evaluation "맛보기" — action 이 오기 전까지 실행 안 한다
#   Lesson 02: select, withColumn, filter, orderBy — 행 단위 변환
#   Lesson 03: groupBy/agg, shuffle, Stage 경계
#   Lesson 04: 왜 lazy인가 → Catalyst 가 plan을 모아서 한 번에 최적화하기 때문
#
# =============================================================================
# [핵심 개념 1] Lazy Evaluation 재정의 — "왜" lazy인가
# =============================================================================
#
#  Lesson 01 에서 배운 것: "action 이 오기 전까지 실행하지 않는다"
#  이번 레슨의 깊이:     "왜 그렇게 설계됐는가?"
#
#  답: Catalyst Optimizer 가 global optimization 을 하려면
#      transformation 의 전체 목록(plan)이 필요하다.
#      조각조각 즉시 실행하면 전체를 보고 최적화할 수 없다.
#
# ─────────────────────────────────────────────────────────────
#  [만약 Eager (즉시 실행) 였다면]
#
#    df.filter(salary > 5000)   → 즉시 실행: 전체 N행 스캔 + 결과 생성
#    .select("name")            → 또 실행: 필터 결과 재스캔 + 새 DataFrame
#    .groupBy("dept").agg(...)  → 또 실행: 또 스캔
#
#    데이터를 3번 읽음, 중간 결과물 2개 생성
#
# ─────────────────────────────────────────────────────────────
#  [Lazy (실제 Spark)]
#
#    df.filter(salary > 5000)   → plan 에 추가만 됨 (실행 X)
#    .select("name")            → plan 에 추가만 됨 (실행 X)
#    .groupBy("dept").agg(...)  → plan 에 추가만 됨 (실행 X)
#                                         ↓ action 호출 시
#    Catalyst 가 plan 전체를 보고 최적화:
#    "Scan(name, dept, salary) → Filter(salary>5000) → Agg"
#
#    데이터를 1번만 읽음, 중간 결과물 없음
# ─────────────────────────────────────────────────────────────
#
# =============================================================================
# [핵심 개념 2] explain() — Physical Plan 읽는 법
# =============================================================================
#
#  explain() 은 Catalyst 가 최종적으로 어떻게 실행할지를 트리로 보여준다.
#  트리는 "아래에서 위로" 읽는다:
#    가장 아래 = 데이터 소스 (Scan)
#    중간      = 변환 단계들 (Filter, Project, Agg)
#    가장 위   = 결과 출력
#
#  주요 키워드:
#    LocalTableScan   = 인메모리 DataFrame 읽기
#    Scan parquet     = Parquet 파일 읽기
#    Filter           = 조건 필터
#    Project          = select (컬럼 선택)
#    HashAggregate    = groupBy/agg
#    Exchange         = shuffle (Stage 경계)
#
# =============================================================================

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time

# =============================================================================
# Step 1: SparkSession 생성
# =============================================================================

spark = (
    SparkSession.builder
    .appName("Lesson04_Demo")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Lesson 04 — Lazy Evaluation 심화 & DAG 시각화")
print("=" * 60)

# =============================================================================
# Step 2: 예제 데이터 (Lesson 03 와 동일한 employees)
# =============================================================================

schema = StructType([
    StructField("name",   StringType(),  nullable=False),
    StructField("dept",   StringType(),  nullable=False),
    StructField("salary", IntegerType(), nullable=False),
    StructField("score",  DoubleType(),  nullable=True),
    StructField("years",  IntegerType(), nullable=False),
])

employees = spark.createDataFrame(
    data=[
        ("김민준", "엔지니어링", 5500, 4.5, 3),
        ("이서연", "마케팅",     4800, 3.9, 5),
        ("박지훈", "엔지니어링", 6200, 4.8, 7),
        ("최유진", "인사",       4200, None, 2),
        ("정하은", "마케팅",     5100, 4.1, 4),
        ("한도현", "엔지니어링", 5800, 4.6, 6),
        ("오지수", "인사",       3900, 3.5, 1),
        ("강태양", "마케팅",     4600, 3.7, 2),
        ("윤하늘", "엔지니어링", 5300, 4.3, 4),
        ("임소율", "인사",       4500, 4.0, 3),
    ],
    schema=schema,
)

print("\n[원본 데이터]")
employees.show(truncate=False)

# =============================================================================
# Step 3: Transformation 은 plan 에 추가만 된다 — 실행 증거
# =============================================================================
#
#  아래 세 줄은 코드가 실행되는 순간 아무 계산도 하지 않는다.
#  plan 에 "할 일 목록"만 쌓인다.
#

print("\n[Step 3] Transformation 만 쌓기 — action 없음")
print("-" * 40)

t1 = employees.filter(F.col("salary") > 5000)     # plan 에 추가만 됨
t2 = t1.select("name", "dept", "salary")           # plan 에 추가만 됨
t3 = t2.groupBy("dept").agg(                       # plan 에 추가만 됨
    F.avg("salary").alias("avg_salary")
)

print("t3 을 만들었지만 아직 실행되지 않았다. Spark UI Jobs 탭을 보면 새 Job 이 없다.")
print("이제 action(.count()) 을 호출해보자.")

count_result = t3.count()    # 여기서 비로소 Catalyst 가 plan 을 최적화 후 실행
print(f"t3.count() 결과: {count_result}  — 이 시점에 Jobs 탭에 Job 이 추가됐을 것이다.")

# =============================================================================
# Step 4: explain() 으로 Physical Plan 읽기
# =============================================================================
#
#  explain() 은 action 없이 호출 가능. Catalyst 가 plan 을 어떻게 실행할지 보여준다.
#  트리를 "아래에서 위로" 읽어라.
#

print("\n[Step 4] explain() — Physical Plan")
print("-" * 40)

simple_query = employees.filter(F.col("salary") > 5000).select("name", "salary")
print(">>> simple_query.explain()")
simple_query.explain()
# 예시 출력 (아래에서 위로 읽는다):
# == Physical Plan ==
# *(1) Project [name#0, salary#2]
# +- *(1) Filter (isnotnull(salary#2) AND (salary#2 > 5000))
#    +- *(1) LocalTableScan [name#0, salary#2]
#
# 읽는 법:
#   1. LocalTableScan   — 인메모리 DataFrame 에서 name, salary 컬럼만 읽음 (projection pruning)
#   2. Filter           — salary > 5000 조건 적용
#   3. Project          — 최종 출력 컬럼 선택

print()
print(">>> groupBy + agg explain()")
agg_query = (
    employees
    .filter(F.col("salary") > 5000)
    .groupBy("dept")
    .agg(F.avg("salary").alias("avg_salary"))
)
agg_query.explain()
# Exchange 노드 = shuffle 발생 지점 (Stage 경계)
# HashAggregate 가 두 번 나온다:
#   첫 번째 (아래): partial aggregation — 각 partition 에서 부분 집계
#   두 번째 (위):   final merge — shuffle 후 최종 집계

# =============================================================================
# Step 5: Catalyst 최적화 증거 — 코드 순서와 상관없이 같은 plan
# =============================================================================
#
#  학습자가 자주 하는 실수: filter 를 select 뒤에 쓰면 비효율적이다?
#  실제로는 Catalyst 가 두 코드를 동일한 Physical Plan 으로 만든다.
#

print("\n[Step 5] Catalyst 최적화 — filter 순서를 바꿔도 같은 plan")
print("-" * 40)

# 코드 A: filter 먼저
query_a = employees.filter(F.col("salary") > 5000).select("name", "salary")
# 코드 B: select 먼저 (filter 가 나중)
query_b = employees.select("name", "salary").filter(F.col("salary") > 5000)

print("--- 코드 A: filter → select ---")
query_a.explain()

print("--- 코드 B: select → filter ---")
query_b.explain()

print("두 Physical Plan 을 비교해보라. 동일하다.")
print("Catalyst 가 filter pushdown 으로 filter 를 scan 바로 위로 끌어내렸기 때문이다.")

# =============================================================================
# Step 6: Parquet 으로 PushedFilters + ReadSchema 증거
# =============================================================================
#
#  인메모리 DataFrame 에서는 LocalTableScan 이라 "데이터 소스까지 내려간다"의
#  극적인 효과를 체감하기 어렵다.
#  Parquet 파일에서 읽으면 Physical Plan 에 두 가지 핵심 줄이 나온다:
#    PushedFilters — 파일 리더가 직접 거른 조건 (디코딩 전 필터링)
#    ReadSchema    — 실제로 읽은 컬럼 목록 (projection pruning 증거)
#

print("\n[Step 6] Parquet PushedFilters + ReadSchema 확인")
print("-" * 40)

parquet_path = "/opt/bitnami/spark/data/lesson04_employees"
# spark-data named volume: master/worker 가 공유하므로 executor 분산 read 가능.
# 호스트에서 직접 보려면 Jupyter (http://localhost:8888) 의 work/data/ 디렉토리 참고.
# 권한 에러 시: docker exec -u 0 spark-master chmod -R 777 /opt/bitnami/spark/data
# (docker-compose 의 spark-data-init 서비스가 부팅 시 자동 적용)

print(f"employees 를 Parquet 으로 저장: {parquet_path}")
employees.write.mode("overwrite").parquet(parquet_path)

df_parquet = spark.read.parquet(parquet_path)

print()
print(">>> Parquet 에서 filter + select explain()")
df_parquet.filter(F.col("salary") > 5000).select("name", "salary").explain()
# Physical Plan 에서 확인할 두 줄:
#
# PushedFilters: [IsNotNull(salary), GreaterThan(salary,5000)]
#   → filter 조건이 Parquet 리더까지 내려가 Parquet row group 단위로 사전 거름
#
# ReadSchema: struct<name:string,salary:int>
#   → select 한 2개 컬럼만 실제로 읽음 (5개 컬럼 중 3개는 디스크조차 안 읽음)

print()
print(">>> 비교: 전체 컬럼 읽을 때 ReadSchema")
df_parquet.filter(F.col("salary") > 5000).explain()
# ReadSchema 에 name, dept, salary, score, years 5개 컬럼 모두 나열됨
# → select 로 컬럼을 줄이면 I/O 자체가 감소함을 확인할 수 있다

# =============================================================================
# Step 7: WHERE vs HAVING — Catalyst 가 합칠 수 있는 것과 없는 것
# =============================================================================
#
#  Catalyst 는 "의미가 같은" 연산만 합친다.
#  WHERE filter (집계 전) 와 HAVING filter (집계 후) 는 의미가 다르므로
#  Physical Plan 이 서로 다르다.
#

print("\n[Step 7] WHERE vs HAVING — 의미가 다르면 Catalyst 도 다르게 만든다")
print("-" * 40)

# WHERE: 집계 전 행 필터 (salary 는 원본 컬럼)
q_where = (
    employees
    .filter(F.col("salary") > 5000)          # WHERE salary > 5000
    .groupBy("dept")
    .agg(F.avg("salary").alias("avg_salary"))
)

# HAVING: 집계 후 그룹 필터 (max_salary 는 집계된 컬럼)
q_having = (
    employees
    .groupBy("dept")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("max_salary") > 5000)      # HAVING max_salary > 5000
)

print("--- WHERE filter (집계 전) ---")
q_where.explain()

print("--- HAVING filter (집계 후) ---")
q_having.explain()
# q_where: Filter 가 HashAggregate 아래에 위치 (먼저 행을 걸러낸 뒤 집계)
# q_having: Filter 가 HashAggregate 위에 위치 (집계한 뒤 그룹을 걸러냄)
# Catalyst 가 의미를 바꾸지 않으므로 두 plan 이 다르다.

print()
print("[결과 비교]")
print("WHERE (salary > 5000 인 직원만 집계):")
q_where.show()
print("HAVING (max_salary > 5000 인 부서만 남김):")
q_having.show()

# =============================================================================
# [더 알고 싶다면] explain 의 다른 모드
# =============================================================================
#
#  explain()            = Physical Plan 만 (입문자 추천)
#
#  explain("formatted") = Physical Plan 을 두 부분으로 출력
#                         상단: 트리 요약
#                         하단: 각 노드별 상세 정보 (Input/Output 컬럼, Condition 등)
#
#  explain(extended=True) 또는 explain("extended") = 4단계 plan 전부 출력
#    == Parsed Logical Plan ==    : 코드 그대로, 아직 검증 안 함
#    == Analyzed Logical Plan ==  : 컬럼 이름·타입 확인 완료
#    == Optimized Logical Plan == : Catalyst 가 rewrite 한 결과
#    == Physical Plan ==          : 실제 실행 알고리즘
#
#  실제로 보고 싶다면:
#    employees.filter(F.col("salary") > 5000).select("name").explain("extended")
#
# =============================================================================

# =============================================================================
# [Spark UI 에서 확인할 것] — localhost:4040
# =============================================================================
#
#  1. SQL / DataFrame 탭 클릭
#     - 이 스크립트에서 실행된 쿼리 목록이 보인다.
#     - 쿼리 클릭 → 노드 그래프 = explain() 의 시각적 버전
#     - "Exchange" 노드 = shuffle 발생 지점 (Stage 경계)
#     - "Filter" 노드 위치 = pushdown 됐는지 확인 (Scan 바로 위면 pushdown)
#
#  2. Jobs 탭 클릭
#     - show(), count() 같은 action 마다 Job 1개가 생성됐다.
#     - Step 3 에서 transformation 만 쌓을 때는 Job 이 없었고,
#       count() 호출 후에 Job 이 추가된 것을 확인하라.
#     - Job 클릭 → DAG Visualization: Stage 경계와 각 Stage 내 연산 그래프
#
#  3. Stages 탭
#     - groupBy 를 포함한 Job 은 2개 Stage 로 쪼개진다.
#     - Stage 0: partial aggregation + shuffle write
#     - Stage 1: shuffle read + final aggregation
#
#  주의: SparkSession 이 살아있는 동안만 localhost:4040 접근 가능.
#        아래 sleep 이 끝나기 전에 확인하세요.
#
# =============================================================================

print("\n" + "=" * 60)
print("Lesson 04 Demo 완료!")
print("localhost:4040 에서 Spark UI 를 확인하세요.")
print("SQL/DataFrame 탭: 쿼리별 노드 그래프")
print("Jobs 탭: action 마다 생긴 Job 과 DAG")
print("60초 후 자동 종료됩니다. (Ctrl+C 로 즉시 종료 가능)")
print("=" * 60)

time.sleep(60)
spark.stop()
