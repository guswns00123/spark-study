# =============================================================================
# Lesson 03 — 연습 파일 (빈칸 채우기)
# =============================================================================
#
# 실행 명령 (PowerShell):
#   docker exec spark-master spark-submit `
#     --master spark://spark-master:7077 `
#     /opt/bitnami/spark/examples/local/lesson_03_practice.py
#
# ___ 가 있는 곳을 채워보세요. 힌트는 각 TODO 블록에 있습니다.
#
# [모범 답안 체크리스트]
#   TODO 1  groupBy + count() 로 부서별 직원 수 구하기
#   TODO 2  agg() 안에 여러 집계 함수 + alias() 적용
#   TODO 3  집계 결과에 filter() 로 HAVING 조건 적용
#   TODO 4  orderBy(F.desc()) 로 집계 결과 내림차순 정렬
#   TODO 5  실전 파이프라인 완성 (groupBy + agg + filter + orderBy)
#
# =============================================================================

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = (
    SparkSession.builder
    .appName("Lesson03_Practice")
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
print("원본 데이터")
print("=" * 60)
employees.show(truncate=False)

# =============================================================================
# TODO 1: groupBy + count() — 부서별 직원 수
# =============================================================================
#
# 목표: 각 부서(dept)에 직원이 몇 명인지 세세요.
#
# 힌트:
#   df.groupBy("기준컬럼").count()
#   count() 는 agg() 없이 바로 붙일 수 있는 유일한 집계 메서드.
#

print("\n[TODO 1] 부서별 직원 수")
print("-" * 40)
result_1 = employees.groupBy('dept').count()   # TODO 1: 기준 컬럼을 채우세요
result_1.show()

# [기대 출력] (순서는 달라도 됨)
# +-----------+-----+
# |dept       |count|
# +-----------+-----+
# |엔지니어링 |4    |
# |마케팅     |3    |
# |인사       |3    |
# +-----------+-----+

# =============================================================================
# TODO 2: groupBy + agg() — 여러 집계 함수 동시 적용
# =============================================================================
#
# 목표: 부서별로 아래 4가지를 한 번에 구하세요.
#   - 직원 수       → 컬럼명: "headcount"
#   - 평균 연봉     → 컬럼명: "avg_salary"
#   - 최고 연봉     → 컬럼명: "max_salary"
#   - 평균 평가점수 → 컬럼명: "avg_score"
#
# 힌트:
#   .agg(
#       F.count("name").alias("headcount"),
#       F.avg("salary").alias("avg_salary"),
#       ...
#   )
#   집계 함수: F.count(), F.avg(), F.sum(), F.max(), F.min()
#

print("\n[TODO 2] 부서별 다중 집계 (headcount, avg_salary, max_salary, avg_score)")
print("-" * 40)
result_2 = (
    employees
    .groupBy("dept")
    .agg(
        F.count('name').alias('headcount'),          # TODO 2a: 직원 수 (headcount)
        F.avg('salary').alias('avg_salary'),            # TODO 2b: 평균 연봉 (avg_salary)
        F.max('salary').alias('max_salary'),            # TODO 2c: 최고 연봉 (max_salary)
        F.avg('score').alias('avg_score'),            # TODO 2d: 평균 점수 (avg_score)
    )
)
result_2.show(truncate=False)

# [기대 출력]
# +-----------+---------+------------------+-----------+------------------+
# |dept       |headcount|avg_salary        |max_salary |avg_score         |
# +-----------+---------+------------------+-----------+------------------+
# |엔지니어링 |4        |5700.0            |6200       |4.55              |
# |마케팅     |3        |4833.333...       |5100       |3.9               |
# |인사       |3        |4200.0            |4500       |3.75              |
# +-----------+---------+------------------+-----------+------------------+

# =============================================================================
# TODO 3: 집계 후 filter() — SQL 의 HAVING
# =============================================================================
#
# 목표:
#   - 부서별 직원 수(headcount)와 평균 연봉(avg_salary)을 구한다.
#   - 그 중 직원 수가 3명 이상인 부서만 남긴다.  ← 이것이 HAVING 절
#
# 핵심 주의:
#   groupBy 이후에는 집계 컬럼(headcount, avg_salary)만 남습니다.
#   name, score 같은 원본 컬럼은 사라지므로,
#   filter 조건도 집계 컬럼으로 써야 합니다.
#
# 힌트:
#   .filter(F.col("집계컬럼") >= 값)
#

print("\n[TODO 3] 집계 후 filter: 직원 수 3명 이상인 부서만")
print("-" * 40)
result_3 = (
    employees
    .groupBy("dept")
    .agg(
        F.count("name").alias("headcount"),
        F.avg("salary").alias("avg_salary"),
    )
    .filter(F.col('headcount') >= 3)    # TODO 3: headcount >= 3 조건을 채우세요
)
result_3.show(truncate=False)

# [기대 출력] — 모든 부서가 3명이므로 3개 부서 모두 나와야 함
# +-----------+---------+------------------+
# |dept       |headcount|avg_salary        |
# +-----------+---------+------------------+
# |엔지니어링 |4        |5700.0            |
# |마케팅     |3        |4833.333...       |
# |인사       |3        |4200.0            |
# +-----------+---------+------------------+

# =============================================================================
# TODO 4: orderBy(F.desc()) 로 집계 결과 내림차순 정렬
# =============================================================================
#
# 목표: 부서별 평균 연봉을 구하고, 평균 연봉이 높은 부서를 먼저 출력하세요.
#
# 힌트:
#   방법 A: .orderBy(F.col("avg_salary").desc())
#   방법 B: .orderBy(F.desc("avg_salary"))     ← 더 짧음, 동일한 결과
#

print("\n[TODO 4] 평균 연봉 내림차순 정렬")
print("-" * 40)
result_4 = (
    employees
    .groupBy("dept")
    .agg(F.avg("salary").alias("avg_salary"))
    .orderBy(F.desc('avg_salary'))    # TODO 4: 평균 연봉 내림차순 정렬 (F.desc 활용)
)
result_4.show(truncate=False)

# [기대 출력]
# +-----------+------------------+
# |dept       |avg_salary        |
# +-----------+------------------+
# |엔지니어링 |5700.0            |
# |마케팅     |4833.333...       |
# |인사       |4200.0            |
# +-----------+------------------+

# =============================================================================
# TODO 5: 실전 파이프라인 — groupBy + agg + filter + orderBy 완성
# =============================================================================
#
# 목표: 아래 파이프라인을 완성하세요.
#   1. 부서별로 집계:
#        headcount   = 직원 수
#        avg_salary  = 평균 연봉
#        total_salary= 연봉 합계
#        top_score   = 최고 평가 점수
#   2. 평균 연봉(avg_salary)이 4500 이상인 부서만 남기기
#   3. 평균 연봉 내림차순으로 정렬
#
# 힌트:
#   F.sum("컬럼")   → 합계
#   F.max("컬럼")   → 최댓값
#

print("\n[TODO 5] 실전 파이프라인: 집계 + 필터 + 정렬")
print("-" * 40)
result_5 = (
    employees
    .groupBy('dept')                                      # TODO 5a: 기준 컬럼
    .agg(
        F.count("name").alias("headcount"),
        F.avg("salary").alias("avg_salary"),
        F.sum('salary').alias("total_salary"),              # TODO 5b: 연봉 합계
        F.max('score').alias("top_score"),                 # TODO 5c: 최고 점수
    )
    .filter(F.col('avg_salary') >= 4500)                        # TODO 5d: avg_salary >= 4500
    .orderBy(F.desc('avg_salary'))                              # TODO 5e: avg_salary 내림차순
)
result_5.show(truncate=False)

# [기대 출력]
# +-----------+---------+------------------+------------+---------+
# |dept       |headcount|avg_salary        |total_salary|top_score|
# +-----------+---------+------------------+------------+---------+
# |엔지니어링 |4        |5700.0            |22800       |4.8      |
# |마케팅     |3        |4833.333...       |14500       |4.1      |
# +-----------+---------+------------------+------------+---------+
# (인사 부서 avg_salary = 4200 → 4500 미만이므로 제외)

# =============================================================================
# [보너스 퀴즈 — 도전해보세요]
# =============================================================================
#
# Q. 아래 SQL 을 DataFrame API 로 변환하면?
#
#   SELECT dept, COUNT(*) AS headcount, SUM(salary) AS total_salary
#   FROM employees
#   WHERE years >= 3          ← groupBy 이전에 행 필터
#   GROUP BY dept
#   HAVING headcount >= 2     ← groupBy 이후 집계 필터
#   ORDER BY total_salary DESC
#
# 힌트: filter → groupBy → agg → filter → orderBy 순서로 체인.
#
result_bonus = (
    employees
    .filter(F.col('years') >= 3)                  # WHERE years >= 3
    .groupBy('dept')                  # GROUP BY dept
    .agg(F.count('name').alias('headcount'),
    F.sum('salary').alias('total_salary') )# SELECT + HAVING 집계
    .filter(F.col("headcount") >= 2)                  # HAVING headcount >= 2
    .orderBy(F.desc('total_salary'))                  # ORDER BY total_salary DESC
)
result_bonus.show(truncate=False)

# =============================================================================
print("\n" + "=" * 60)
print("연습 완료! 모든 TODO 를 채웠나요?")
print("demo 파일(lesson_03_demo.py)과 출력을 비교해 보세요.")
print("localhost:4040 → Stages 탭 → Shuffle Read/Write 컬럼을 확인하세요.")
print("=" * 60)

spark.stop()
