# =============================================================================
# Lesson 03 — groupBy, agg, 집계 함수, Narrow vs Wide, Shuffle
# =============================================================================
#
# [이 레슨에서 배우는 것]
#   1. groupBy() + agg() 기본 사용법
#   2. count, sum, avg, min, max 집계 함수 (pyspark.sql.functions)
#   3. agg() 안에서 여러 집계를 동시에 + alias() 로 컬럼 이름 정리
#   4. Narrow vs Wide transformation — groupBy 는 왜 "wide"인가
#   5. Shuffle 이 왜 비싼가 (네트워크 I/O, 디스크 spill)
#   6. 집계 결과에 filter() 적용하기 (SQL 의 HAVING 과 동일)
#   7. orderBy(F.desc()) 로 집계 결과 정렬
#   8. Spark UI Stages 탭에서 Shuffle Read/Write 확인 방법
#
# [Lesson 01~02 와의 연결]
#   Lesson 01: DataFrame 만들기, lazy evaluation, action 트리거
#   Lesson 02: select, withColumn, filter, orderBy — 행 단위 변환
#   Lesson 03: 여러 행을 "합쳐서" 요약하는 집계 연산 + shuffle 개념
#
# =============================================================================
# [핵심 개념 1] Narrow vs Wide Transformation
# =============================================================================
#
#  Narrow: 입력 partition → 출력 partition 이 1:1 대응
#          데이터가 파티션 경계를 넘지 않는다.
#          예: filter, select, withColumn, map
#
#  Wide  : 입력 partition 여러 개 → 출력 partition 1개 이상으로 재편
#          같은 key 를 가진 행이 서로 다른 executor 에 흩어져 있을 수 있으므로
#          네트워크를 통해 데이터를 재분배(shuffle)해야 한다.
#          예: groupBy, orderBy, join, distinct
#
#  ASCII 다이어그램:
#
#  [Narrow — filter]                  [Wide — groupBy]
#
#  Partition 0  →  Partition 0        Partition 0 ──┐
#  [ A, B, C ]     [ A, C ]           [ 엔지니어링  │  SHUFFLE
#                                       마케팅      │  (네트워크 전송)
#  Partition 1  →  Partition 1          인사 ]      │
#  [ D, E, F ]     [ D, F ]           Partition 1 ──┤
#                                     [ 엔지니어링  │
#  각 파티션이 독립적으로 처리.          마케팅 ]    │
#  네트워크 I/O 없음.                  Partition 2 ──┘
#                                             ↓
#                                     [ 엔지니어링 → (합산)
#                                       마케팅     → (합산)
#                                       인사       → (합산) ]
#
# =============================================================================
# [핵심 개념 2] Shuffle 이 왜 비싼가
# =============================================================================
#
#  1. 네트워크 I/O: 각 executor 가 자신의 데이터를 다른 executor 로 전송.
#                   데이터가 클수록 시간이 오래 걸린다.
#
#  2. 디스크 spill : 메모리가 부족하면 중간 데이터를 디스크에 씀.
#                    I/O 비용이 두 배로 뜀 (write + read).
#
#  3. Stage 경계  : Shuffle 은 반드시 Stage 를 나눈다.
#                   앞 Stage 가 끝나야 뒤 Stage 가 시작.
#                   (Spark UI 에서 Stage 가 2개로 쪼개지는 이유)
#
#  Spark UI → Jobs 탭 → 해당 Job 클릭 → Stages 탭 확인
#  "Shuffle Read" / "Shuffle Write" 컬럼에 숫자가 있으면 shuffle 발생!
#
# =============================================================================

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# =============================================================================
# Step 1: SparkSession 생성
# =============================================================================

spark = (
    SparkSession.builder
    .appName("Lesson03_Demo")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Lesson 03 — groupBy, 집계 함수, Shuffle")
print("=" * 60)

# =============================================================================
# Step 2: 예제 데이터 (Lesson 02 employees 확장 — 부서 추가)
# =============================================================================

schema = StructType([
    StructField("name",   StringType(),  nullable=False),
    StructField("dept",   StringType(),  nullable=False),
    StructField("salary", IntegerType(), nullable=False),  # 만원 단위
    StructField("score",  DoubleType(),  nullable=True),   # 평가 점수
    StructField("years",  IntegerType(), nullable=False),  # 근속 연수
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
# Step 3: groupBy() + 단일 집계 함수
# =============================================================================
#
#  groupBy("기준컬럼").집계함수()
#  가장 간단한 패턴:
#
#    df.groupBy("dept").count()
#
#  count() 는 agg() 없이 바로 붙일 수 있는 예외적인 메서드.
#  sum, avg, min, max 는 agg() 안에서 F.함수() 로 써야 한다.
#

print("\n[groupBy + count] 부서별 직원 수")
print("-" * 40)
employees.groupBy("dept").count().show()
# 출력 컬럼명: dept, count
# count() 는 자동으로 "count" 라는 이름 붙여줌

# =============================================================================
# Step 4: groupBy() + agg() — 여러 집계 한 번에
# =============================================================================
#
#  agg() 안에 F.함수() 를 쉼표로 나열하면 한 번의 groupBy 에
#  여러 집계를 동시에 수행할 수 있다.
#
#  반드시 alias() 를 붙여야 컬럼 이름이 깔끔해진다!
#
#  alias() 없으면:  avg(salary)  sum(salary)  max(score)
#  alias() 있으면:  avg_salary   total_salary  max_score
#

print("\n[groupBy + agg] 부서별 다중 집계")
print("-" * 40)
dept_stats = (
    employees
    .groupBy("dept")
    .agg(
        F.count("name").alias("headcount"),         # 직원 수
        F.avg("salary").alias("avg_salary"),         # 평균 연봉
        F.sum("salary").alias("total_salary"),       # 연봉 합계
        F.max("salary").alias("max_salary"),         # 최고 연봉
        F.min("salary").alias("min_salary"),         # 최저 연봉
        F.avg("score").alias("avg_score"),           # 평균 점수
    )
)
dept_stats.show(truncate=False)

# =============================================================================
# Step 5: alias() 로 컬럼 이름 정리 — 왜 중요한가
# =============================================================================
#
#  alias() 를 붙이지 않으면 컬럼 이름이 "avg(salary)" 처럼
#  함수 시그니처 그대로 나온다.
#  이 이름으로 나중에 col() 참조를 하려면 백틱(`) 또는 따옴표 처리가 필요해서
#  코드가 지저분해진다. 항상 alias() 를 붙이는 습관을 들이자.
#

print("\n[alias 없는 경우 vs 있는 경우 비교]")
print("-" * 40)

print("--- alias 없음 ---")
employees.groupBy("dept").agg(F.avg("salary")).show()
# 컬럼명이 "avg(salary)" 로 나옴 → 나중에 col("avg(salary)") 로 참조해야 해서 불편

print("--- alias 있음 ---")
employees.groupBy("dept").agg(F.avg("salary").alias("avg_salary")).show()
# 컬럼명이 "avg_salary" 로 나옴 → col("avg_salary") 로 깔끔하게 참조 가능

# =============================================================================
# Step 6: 집계 결과에 filter() 적용 — SQL 의 HAVING 과 동일
# =============================================================================
#
#  SQL:
#    SELECT dept, COUNT(*) AS headcount
#    FROM employees
#    GROUP BY dept
#    HAVING headcount >= 3
#
#  Spark DataFrame:
#    employees
#      .groupBy("dept")
#      .agg(F.count("name").alias("headcount"))
#      .filter(col("headcount") >= 3)          ← groupBy 이후에 filter 적용
#
#  groupBy 로 만들어진 새 DataFrame 은 집계 컬럼만 남는다.
#  따라서 filter 도 집계 컬럼(headcount, avg_salary 등)을 기준으로 써야 한다.
#  원본 employees 컬럼(name, score 등)은 groupBy 이후 사라진다!
#

print("\n[집계 후 filter — HAVING] 직원 수 3명 이상인 부서만")
print("-" * 40)
(
    employees
    .groupBy("dept")
    .agg(F.count("name").alias("headcount"))
    .filter(F.col("headcount") >= 3)          # HAVING headcount >= 3
).show(truncate=False)

# =============================================================================
# Step 7: orderBy(F.desc()) 로 집계 결과 정렬
# =============================================================================
#
#  집계 결과를 avg_salary 내림차순으로 보고 싶을 때:
#    .orderBy(F.col("avg_salary").desc())
#  또는:
#    .orderBy(F.desc("avg_salary"))   ← F.desc() 함수 방식
#
#  두 방식은 동일하다. F.desc("컬럼명") 이 타이핑이 더 짧다.
#

print("\n[groupBy + agg + orderBy] 평균 연봉 높은 부서 순 정렬")
print("-" * 40)
(
    employees
    .groupBy("dept")
    .agg(
        F.count("name").alias("headcount"),
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
    .orderBy(F.desc("avg_salary"))            # 평균 연봉 내림차순
).show(truncate=False)

# =============================================================================
# Step 8: 실전 파이프라인 — 집계 + 필터 + 정렬
# =============================================================================
#
#  목표:
#    - 부서별 평균 연봉, 직원 수, 최고 점수를 구한다.
#    - 평균 연봉이 4500 이상인 부서만 남긴다.
#    - 평균 연봉 내림차순으로 정렬한다.
#

print("\n[실전 파이프라인] 부서별 요약 → 조건 필터 → 정렬")
print("-" * 40)
result = (
    employees
    .groupBy("dept")
    .agg(
        F.count("name").alias("headcount"),
        F.avg("salary").alias("avg_salary"),
        F.max("score").alias("top_score"),
    )
    .filter(F.col("avg_salary") >= 4500)      # HAVING avg_salary >= 4500
    .orderBy(F.desc("avg_salary"))
)
result.show(truncate=False)

# =============================================================================
# [Spark UI 에서 확인할 것] — localhost:4040 (클러스터면 spark-master:4040)
# =============================================================================
#
#  1. Jobs 탭
#     - groupBy + agg 를 포함한 job 은 "2개 Stage" 로 쪼개진다.
#     - Stage 0: 각 파티션에서 partial aggregation (map side)
#     - Stage 1: shuffle 후 최종 aggregation (reduce side)
#
#  2. Stages 탭 → 각 Stage 클릭
#     - "Shuffle Write" 숫자가 있는 Stage = 데이터를 다음 Stage 로 내보낸 쪽
#     - "Shuffle Read"  숫자가 있는 Stage = 다른 Stage 에서 받은 데이터 크기
#     - 이 숫자가 클수록 shuffle 비용이 크다.
#
#  3. SQL / DataFrame 탭
#     - "HashAggregate" 노드 두 개가 보인다.
#       첫 번째: partial_avg, partial_sum … (각 파티션 내 부분 집계)
#       두 번째: avg, sum … (최종 merge)
#     - "Exchange" 노드 = shuffle 이 발생하는 지점
#
# =============================================================================

print("\n" + "=" * 60)
print("Lesson 03 Demo 완료!")
print("localhost:4040 → Stages 탭 → Shuffle Read/Write 컬럼을 확인하세요.")
print("=" * 60)

spark.stop()
