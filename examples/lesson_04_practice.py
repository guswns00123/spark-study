# =============================================================================
# Lesson 04 — 연습 파일 (예측 → 실행 → 검증)
# =============================================================================
#
# 실행 명령 (Docker):
#   docker exec spark-master spark-submit `
#     --master spark://spark-master:7077 `
#     /opt/bitnami/spark/examples/local/lesson_04_practice.py
#
# 이 파일의 학습 방식:
#   각 TODO 는 3단계로 진행한다.
#   1. [예측] 코드를 실행하기 전에 결과를 주석으로 먼저 써본다.
#   2. [실행] 코드를 실행한다.
#   3. [검증] 예측과 실제 결과를 비교하고, 차이가 있다면 이유를 주석으로 적는다.
#
# [TODO 목록]
#   TODO A  filter→select 순서 vs select→filter 순서, explain() 비교
#   TODO B  Parquet 에서 PushedFilters + ReadSchema 찾아 주석에 옮겨 적기
#   TODO C  WHERE filter vs HAVING filter — Catalyst 가 왜 다르게 만드는가
#   TODO D  (도전) action 없으면 Job 이 생기는가? Spark UI 에서 확인
#
# =============================================================================

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time

spark = (
    SparkSession.builder
    .appName("Lesson04_Practice")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# 데이터 준비 (건드리지 마세요)
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

print("=" * 60)
print("Lesson 04 연습 — 예측 → 실행 → 검증")
print("=" * 60)

# =============================================================================
# TODO A: filter → select 순서 vs select → filter 순서
# =============================================================================
#
# 목표: 두 코드의 explain() 출력을 비교하고,
#       Catalyst 가 어떤 최적화를 했는지 이해한다.
#
# [예측] 코드 A 와 코드 B 의 Physical Plan 이 같을까, 다를까?
# 예측을 여기에 써보세요: 같다
#
# 힌트:
#   filter pushdown = Catalyst 가 filter 를 scan 에 최대한 가깝게 옮기는 것
#   두 코드 중 어느 쪽이 더 "이상적인" 실행 순서와 가까울까?
#

print("\n[TODO A] filter↔select 순서 바꿔도 같은 plan?")
print("-" * 40)

# 코드 A: filter 먼저
query_a = employees.filter(F.col("salary") > 5000).select("name", "salary")

# 코드 B: select 먼저 (filter 가 나중)
query_b = employees.select("name", "salary").filter(F.col("salary") > 5000)

print("--- 코드 A: filter → select ---")
query_a.explain()   # TODO A-1: 코드를 실행해 출력을 보세요

print("--- 코드 B: select → filter ---")
query_b.explain()   # TODO A-2: 코드를 실행해 출력을 보세요

# [검증] 두 Physical Plan 이 같은가, 다른가? 같다
# [왜] Catalyst 의 Physical Plan 최적화 때문이다.
#
# [기대 출력 키워드]
#   두 plan 에 공통으로 등장해야 할 키워드:
#     Filter (isnotnull(salary) AND (salary > 5000))
#     LocalTableScan [name#..., salary#...]

# =============================================================================
# TODO B: Parquet 에서 PushedFilters + ReadSchema 확인
# =============================================================================
#
# 목표: employees 를 Parquet 으로 저장한 뒤 다시 읽고,
#       explain() 출력에서 PushedFilters 와 ReadSchema 두 줄을 찾아 주석에 옮겨 적는다.
#
# [예측] 인메모리 DataFrame 과 Parquet 에서 읽은 DataFrame 의 explain() 가
#         어떻게 다를까? Parquet 쪽 explain 에만 PushedFilters / ReadSchema 두 줄이 추가로 등장 ← (정답)
#
# 힌트:
#   인메모리: LocalTableScan    ← 이미 메모리에 있으므로 pushdown 개념이 없음
#   Parquet : Scan parquet ...  ← 파일 리더가 filter 조건을 직접 처리할 수 있음
#

print("\n[TODO B] Parquet PushedFilters + ReadSchema 확인")
print("-" * 40)

parquet_path = "/opt/bitnami/spark/data/lesson04_employees"
# spark-data named volume (docker-compose.yml 참조) — master/worker 공유 경로.

# Parquet 저장 (이미 demo 에서 저장됐으면 덮어쓰지 않아도 됨)
employees.write.mode("overwrite").parquet(parquet_path)

df_parquet = spark.read.parquet(parquet_path)

print(">>> df_parquet.filter(salary > 5000).select('name', 'salary').explain()")
df_parquet.filter(F.col("salary") > 5000).select("name", "salary").explain()
# TODO B: 출력에서 아래 두 줄을 찾아 ___ 에 옮겨 적으세요.
#
# PushedFilters:  [IsNotNull(salary), GreaterThan(salary,5000)]  ← (정답)
# ReadSchema:     struct<name:string,salary:int>  ← (정답)
#
# [기대 출력 키워드]
#   PushedFilters: [IsNotNull(salary), GreaterThan(salary,5000)]
#   ReadSchema: struct<name:string,salary:int>
#   (컬럼 순서나 id 번호는 달라도 됨)

# =============================================================================
# TODO C: WHERE vs HAVING — Catalyst 가 왜 다르게 만드는가
# =============================================================================
#
# 목표: groupBy 앞의 filter (WHERE) 와 groupBy 뒤의 filter (HAVING) 가
#       Physical Plan 에서 어떻게 다른지 확인한다.
#
# [예측] 두 코드의 Physical Plan 이 같을까, 다를까?
# 예측을 여기에 써보세요: 다르다 ← (정답)
#
# 힌트:
#   q_where: salary > 5000 인 행만 뽑아서 집계 → 집계 전 행이 줄어든다
#   q_having: 모든 행을 집계한 뒤 max_salary > 5000 인 그룹만 남김 → 결과가 다를 수 있다
#

print("\n[TODO C] WHERE filter vs HAVING filter")
print("-" * 40)

# WHERE filter: groupBy 앞에 걸어 집계 전에 행을 줄임
q_where = (
    employees
    .filter(F.col("salary") > 5000)          # WHERE salary > 5000
    .groupBy("dept")
    .agg(F.avg("salary").alias("avg_salary"))
)

# HAVING filter: 집계 결과에 조건. max_salary 는 집계 후에만 존재하는 컬럼
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
q_where.explain()   # TODO C-1: Filter 가 HashAggregate 의 위? 아래?

print("--- HAVING filter (집계 후) ---")
q_having.explain()  # TODO C-2: Filter 가 HashAggregate 의 위? 아래?

# [검증] 두 Physical Plan 이 같은가, 다른가? 다르다 ← (정답)
# [WHERE 의 Filter 위치] HashAggregate 의 아래 (위 / 아래) ← (정답)
# [HAVING 의 Filter 위치] HashAggregate 의 위 (위 / 아래) ← (정답)
# [왜 Catalyst 가 합치지 않는가?] WHERE 는 원본 컬럼(salary) 행 필터, HAVING 은 집계 컬럼(max_salary) 그룹 필터 — 의미가 달라서 합칠 수 없음 ← (정답)
#
# [기대 출력 키워드]
#   q_where Physical Plan:
#     Filter 가 Exchange (shuffle) 아래에 위치
#       → shuffle 전에 데이터를 먼저 줄임
#   q_having Physical Plan:
#     Filter 가 Exchange 위에 위치
#       → 집계 후 그룹 수준에서 필터링

print("\n[결과도 비교해보자]")
print("WHERE (salary > 5000 인 직원만 집계):")
q_where.show()

print("HAVING (max_salary > 5000 인 부서만):")
q_having.show()

# [결과가 다른 이유를 주석으로 설명해보세요] WHERE 는 salary>5000 인 직원만 추려 평균 계산(엔/마 일부만 포함). HAVING 은 전체 직원으로 평균 낸 뒤 max_salary>5000 인 부서만 남김(인사 부서는 max=4500 이라 제외). 평균값과 남는 부서가 다름 ← (정답)

# =============================================================================
# TODO D (도전): action 없으면 Job 이 생기는가?
# =============================================================================
#
# 목표: transformation 만 있을 때와 action 이 있을 때
#       Spark UI Jobs 탭에 어떤 차이가 생기는지 확인한다.
#
# 주의: 인메모리 DataFrame(employees) 만 사용한다.
#       Parquet 읽기는 스키마 추론으로 Job 이 별도 생길 수 있으므로 섞지 않는다.
#
# [예측] transformation 만 쌓으면 Jobs 탭에 Job 이 생길까, 안 생길까? 안생김
#

print("\n[TODO D] action 없으면 Job 이 생기는가? (인메모리 전용)")
print("-" * 40)
print("지금 localhost:4040 → Jobs 탭을 열어두고 아래 코드 실행 과정을 지켜보세요.")

# transformation 만 (action 없음) — Jobs 탭에 아무 변화가 없어야 한다
t_no_action = employees.filter(F.col("salary") > 5000).select("name")
print("transformation 만 쌓음 (t_no_action). Jobs 탭 확인...")
# [검증] 새 Job 이 생겼는가? 안생김

# action 추가 — 이 줄에서 Job 이 생겨야 한다
count_val = employees.filter(F.col("salary") > 5000).select("name").count()
print(f"count() 호출 결과: {count_val}. Jobs 탭에 새 Job 이 생겼을 것이다.")
# [검증] 새 Job 이 생겼는가? 생김
# [Job 번호(ID)를 Jobs 탭에서 찾아 적으세요] ___ ← (실제 Spark UI 에서 본 번호로. count() 가 첫 action 이면 Job 0)

# [결론] Lazy evaluation:
#   transformation → 추가 (plan 에 추가 / 실행)
#   action         → 실행 (plan 에 추가 / 실행)

# =============================================================================
# Spark UI 열어두기
# =============================================================================

print("\n" + "=" * 60)
print("연습 완료! 모든 TODO 를 채웠나요?")
print()
print("체크리스트:")
print("  TODO A: filter↔select 순서를 바꿔도 explain() 결과가 같은가?")
print("  TODO B: PushedFilters 와 ReadSchema 두 줄을 주석에 옮겨 적었는가?")
print("  TODO C: WHERE 과 HAVING 의 Filter 위치가 다른 이유를 설명할 수 있는가?")
print("  TODO D: action 없이는 Jobs 탭에 Job 이 안 생긴다는 것을 확인했는가?")
print()
print("localhost:4040 에서 Spark UI 를 확인하세요.")
print("60초 후 자동 종료됩니다. (Ctrl+C 로 즉시 종료 가능)")
print("=" * 60)

time.sleep(60)
spark.stop()
