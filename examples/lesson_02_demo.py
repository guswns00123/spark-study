# =============================================================================
# Lesson 02 — 컬럼 표현식 & 내장 함수
# =============================================================================
#
# [이 레슨에서 배우는 것]
#   1. col() / lit() 의 의미, 그리고 df["col"] 과의 차이
#   2. select() 로 원하는 컬럼만 뽑기 / 컬럼 표현식 작성
#   3. withColumn() 으로 파생 컬럼 추가 또는 교체
#   4. when / otherwise 로 조건부 컬럼 만들기
#   5. 자주 쓰는 내장 함수: 문자열 / 수학 / 날짜
#   6. alias() 로 컬럼 이름 바꾸기
#   7. Column 객체가 "lazy 표현식" 이라는 점 — 실제 값이 아니다
#   8. explain() 으로 Catalyst 실행 계획 맛보기
#
# [실행 방법]
#   docker exec spark-master \
#       /opt/bitnami/spark/bin/spark-submit \
#       /opt/bitnami/spark/examples/local/lesson_02_demo.py
#
# [Spark UI 에서 볼 것]
#   - localhost:4040 → SQL/DataFrame 탭 → 각 show() 에 대응하는 쿼리
#   - "Parsed Plan → Optimized Plan" 에서 Catalyst 가 필요 없는 컬럼을
#     자동으로 제거(Column Pruning) 하는 것을 확인
#
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit,
    upper, lower, concat, split, length, trim, initcap,
    round as spark_round, abs as spark_abs,
    to_date, year, datediff, current_date,
    when, coalesce,
)

spark = (
    SparkSession.builder
    .appName("Lesson02_Demo")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Lesson 02 — 컬럼 표현식 & 내장 함수")
print("=" * 60)

# =============================================================================
# [핵심 개념 1] col() vs df["컬럼명"] vs 문자열 이름
# =============================================================================
#
# [왜 이걸 배우는가]
#   PySpark 에서 컬럼을 참조하는 방법이 세 가지다.
#   세 방법 모두 "Column 객체" 를 반환하며, 이 객체는 아직 값이 아닌
#   '어떤 컬럼을 어떻게 계산할지' 기술하는 표현식이다.
#   Catalyst Optimizer 는 이 표현식 트리를 최적화해서 물리 실행 계획으로 변환한다.
#
#   col("salary")          : pyspark.sql.functions.col — 어떤 DataFrame 에도 범용
#   employees["salary"]    : 특정 DataFrame 에 묶인 참조
#   "salary"               : 일부 API(select, groupBy)에서 문자열로 직접 전달 가능
#
#   [면접 포인트] col() 은 어떤 DataFrame 에도 쓸 수 있어서 join 뒤처럼
#   컬럼 출처가 모호한 상황에서 명확성을 높여 준다.
#
# [lit()] 은 Python 상수를 Column 객체로 변환한다.
#   withColumn 에서 고정 값 컬럼을 추가할 때 반드시 필요하다.
#   예: lit(1), lit("KR"), lit(None).cast("string")
#
# [Column 객체는 lazy 다]
#   아래 두 줄은 아무것도 실행하지 않는다. 표현식을 정의할 뿐이다.
#   실제 실행은 show() / count() 같은 action 이 호출될 때만 일어난다.
#
salary_col   = col("salary")       # 표현식 정의 — 아직 실행 안 됨
country_col  = lit("KR")           # 상수 표현식 — 아직 실행 안 됨
print(f"\n[col/lit 타입 확인]")
print(f"  type(col('salary')) = {type(salary_col)}")   # Column
print(f"  type(lit('KR'))     = {type(country_col)}")  # Column

# =============================================================================
# Step 1: 예제 데이터 — 직원 테이블
# =============================================================================
#
# [왜 이걸 배우는가]
#   실제 데이터 엔지니어링 업무에서 가장 많이 다루는 도메인 중 하나.
#   이름 정규화, 급여 계산, 이메일 파싱은 면접 코딩 문제에도 자주 등장한다.
#
employees = spark.createDataFrame(
    data=[
        (1, "  kim minjun  ", "Engineering", 5500, 0.10, "kim@company.com",  "2020-03-15", "010-1234-5678"),
        (2, "lee seoyeon",    "Marketing",   4800, 0.05, "lee@startup.io",   "2021-07-01", None),
        (3, "PARK JIHOON",    "Engineering", 6200, 0.15, "park@company.com", "2019-11-20", "010-9876-5432"),
        (4, "choi yujin",     "HR",          4200, 0.08, "choi@corp.net",    "2022-01-10", "010-5555-0000"),
        (5, "jeong haeun",    "Marketing",   5100, 0.12, "jeong@startup.io", "2021-04-25", None),
        (6, "han dohyeon",    "Engineering", 5800, 0.10, "han@company.com",  "2018-06-30", "010-3333-7777"),
    ],
    schema=["id", "name", "dept", "salary", "discount_rate", "email", "hire_date", "phone"],
)
# createDataFrame 은 transformation 이라 아직 실행 안 됨.
# 아래 printSchema() 는 메타데이터만 반환하므로 Executor 가 데이터를 읽지 않는다.
print("\n[Step 1] 직원 테이블 스키마")
print("-" * 40)
employees.printSchema()

# =============================================================================
# Step 2: select() 로 컬럼 뽑기
# =============================================================================
#
# [왜 이걸 배우는가]
#   SQL 의 SELECT 절과 같다. 필요한 컬럼만 뽑으면 Catalyst 가
#   Column Pruning 최적화를 적용해 불필요한 I/O 를 줄인다.
#   세 가지 표기를 섞어서 써도 되지만, 일관성을 위해 col() 을 권장한다.
#
print("\n[Step 2] select — 필요한 컬럼만 선택")
print("-" * 40)
# transformation — 아직 실행 안 됨
selected = employees.select(
    col("id"),
    col("name"),
    col("dept"),
    col("salary"),
)
selected.show(truncate=False)   # show() 가 action → 여기서 실행

# =============================================================================
# Step 3: alias() 로 컬럼 이름 바꾸기
# =============================================================================
#
# [왜 이걸 배우는가]
#   select 나 withColumn 에서 표현식 결과물에 이름을 부여해야
#   이후 체인에서 그 컬럼을 참조할 수 있다.
#   SQL 의 AS 키워드와 동일하다.
#
print("\n[Step 3] alias — 컬럼 이름 변경")
print("-" * 40)
employees.select(
    col("id"),
    col("name").alias("employee_name"),    # 기존 이름 대신 새 이름 부여
    col("salary").alias("salary_10k_krw"), # 단위가 무엇인지 명시
).show(truncate=False)

# =============================================================================
# Step 4: withColumn() 으로 파생 컬럼 추가 / 교체
# =============================================================================
#
# [왜 이걸 배우는가]
#   기존 DataFrame 에 새 컬럼을 추가하거나 기존 컬럼 값을 바꿀 때 쓴다.
#   같은 이름을 쓰면 교체, 새 이름이면 추가다.
#   내부적으로는 새 DataFrame 을 반환(immutable). 원본은 변경되지 않는다.
#
#   [주의] withColumn() 을 수십 번 체인하면 Catalyst 가 만드는 실행 계획이
#   길어져서 컴파일 시간이 늘어날 수 있다. 컬럼이 많으면 select() 로 한번에
#   표현식을 나열하는 게 더 효율적이다. (면접 단골 주의사항)
#
print("\n[Step 4] withColumn — 파생 컬럼 추가")
print("-" * 40)
with_bonus = (
    employees
    # 상수 컬럼 추가: lit() 없이 숫자를 바로 쓰면 오류 발생
    .withColumn("country", lit("KR"))
    # 수식 컬럼 추가: salary * discount_rate 로 보너스 계산
    .withColumn("bonus", col("salary") * col("discount_rate"))
    # 기존 컬럼 교체: bonus 를 정수로 반올림 (소수점 없애기)
    .withColumn("bonus", spark_round(col("bonus"), 0))
)
# transformation 체인 — 아직 아무것도 실행 안 됨
with_bonus.select("id", "name", "salary", "discount_rate", "bonus", "country") \
          .show(truncate=False)   # action

# =============================================================================
# Step 5: when / otherwise — 조건부 컬럼
# =============================================================================
#
# [왜 이걸 배우는가]
#   SQL 의 CASE WHEN 과 동일하다.
#   카테고리 분류, 등급 매기기, null 처리 등 실무에서 매우 자주 쓰인다.
#   when().when().otherwise() 체인으로 다중 조건을 표현한다.
#   otherwise() 를 생략하면 매칭 안 된 행이 null 이 된다.
#
print("\n[Step 5] when/otherwise — 연봉 등급 분류")
print("-" * 40)
with_grade = employees.withColumn(
    "salary_grade",
    when(col("salary") >= 6000, "S등급")
    .when(col("salary") >= 5500, "A등급")
    .when(col("salary") >= 5000, "B등급")
    .otherwise("C등급")
)
with_grade.select("id", "name", "salary", "salary_grade").show(truncate=False)

# =============================================================================
# Step 6: 문자열 내장 함수 — upper / lower / trim / initcap / concat / split
# =============================================================================
#
# [왜 이걸 배우는가]
#   데이터 정제(cleansing) 에서 가장 많이 쓰는 함수들이다.
#   이름 정규화, 이메일 파싱, 코드 표준화가 대표적 사용 예시이며
#   코딩 테스트 단골 소재다.
#
#   - upper(col)   : 모두 대문자
#   - lower(col)   : 모두 소문자
#   - trim(col)    : 앞뒤 공백 제거
#   - initcap(col) : 각 단어의 첫 글자만 대문자 ("kim minjun" → "Kim Minjun")
#   - concat(c1, c2, ...) : 문자열 연결
#   - split(col, pattern) : 정규식 기준 분리, ArrayType 반환
#   - length(col)  : 문자 수
#
print("\n[Step 6] 문자열 함수 — 이름 정규화 + 이메일 도메인 추출")
print("-" * 40)
string_demo = employees.select(
    col("id"),
    col("name").alias("name_original"),
    trim(col("name")).alias("name_trimmed"),          # 공백 제거
    initcap(trim(col("name"))).alias("name_clean"),   # 정규화: trim 후 initcap
    upper(col("dept")).alias("dept_upper"),
    length(col("email")).alias("email_length"),
    # split 은 ArrayType 을 반환. [1] 로 인덱스 접근하면 @ 뒤 도메인만 추출
    split(col("email"), "@")[1].alias("email_domain"),
)
string_demo.show(truncate=False)

# =============================================================================
# Step 7: 수학 함수 — round / abs
# =============================================================================
#
# [왜 이걸 배우는가]
#   금융/통계 데이터에서 소수점 처리와 절댓값은 필수다.
#   Python 의 round() / abs() 가 아닌 Spark 내장 함수를 써야
#   분산 처리 컨텍스트에서 동작한다.
#   (Python 함수는 Column 객체에 적용할 수 없다)
#
print("\n[Step 7] 수학 함수 — round / abs")
print("-" * 40)
math_demo = spark.createDataFrame(
    [("A", 4567.891), ("B", -3210.456), ("C", 100.0)],
    schema=["item", "value"],
)
math_demo.select(
    col("item"),
    col("value"),
    spark_round(col("value"), 1).alias("rounded_1"),    # 소수점 1자리
    spark_round(col("value"), 0).alias("rounded_0"),    # 정수 반올림
    spark_abs(col("value")).alias("abs_value"),
).show()

# =============================================================================
# Step 8: 날짜 함수 — to_date / year / datediff
# =============================================================================
#
# [왜 이걸 배우는가]
#   입사일, 주문일, 만료일 계산은 실무 데이터 분석의 기본이다.
#   StringType 날짜를 DateType 으로 변환해야 날짜 연산을 할 수 있다.
#   to_date() 의 포맷 문자열은 Java SimpleDateFormat 규칙을 따른다.
#
print("\n[Step 8] 날짜 함수 — to_date / year / datediff")
print("-" * 40)
date_demo = employees.select(
    col("id"),
    col("name"),
    col("hire_date"),
    to_date(col("hire_date"), "yyyy-MM-dd").alias("hire_dt"),  # 문자열 → DateType
    year(to_date(col("hire_date"), "yyyy-MM-dd")).alias("hire_year"),
    datediff(
        current_date(),                                        # 오늘 날짜
        to_date(col("hire_date"), "yyyy-MM-dd")
    ).alias("days_since_hire"),
)
date_demo.show(truncate=False)

# =============================================================================
# Step 9: coalesce / when(isNull) — NULL 처리
# =============================================================================
#
# [왜 이걸 배우는가]
#   실무 데이터에는 항상 null 이 있다. null 을 그냥 두면
#   집계, 조인, 비교 연산이 모두 null 을 전파한다.
#   coalesce(c1, c2, ...) 는 첫 번째 non-null 값을 반환한다.
#   when(col.isNull(), "대체값").otherwise(col) 로도 같은 처리를 할 수 있다.
#
#   [면접 포인트] coalesce 와 ifnull(SQL) 의 차이:
#   coalesce 는 인수가 2개 이상 가능, ifnull 은 정확히 2개.
#
print("\n[Step 9] NULL 처리 — coalesce / isNull")
print("-" * 40)
null_demo = employees.select(
    col("id"),
    col("name"),
    col("phone"),
    # coalesce: phone 이 null 이면 'UNKNOWN' 반환
    coalesce(col("phone"), lit("UNKNOWN")).alias("phone_safe"),
    # when + isNull: 동일한 결과지만 더 명시적
    when(col("phone").isNull(), lit("UNKNOWN"))
        .otherwise(col("phone"))
        .alias("phone_when"),
)
null_demo.show(truncate=False)

# =============================================================================
# Step 10: explain() — Catalyst 실행 계획 확인
# =============================================================================
#
# [왜 이걸 배우는가]
#   컬럼 표현식들이 Catalyst Optimizer 에 의해 어떻게 변환되는지
#   눈으로 확인할 수 있다. "Optimized Logical Plan" 에서
#   Project 노드 안에 우리가 만든 표현식이 인라인으로 합쳐진 것을 볼 수 있다.
#   실무에서 느린 쿼리를 디버깅할 때 가장 먼저 사용하는 도구다.
#
print("\n[Step 10] explain() — Catalyst 가 만든 실행 계획")
print("-" * 40)
final_query = employees.select(
    col("id"),
    initcap(trim(col("name"))).alias("name_clean"),
    col("salary"),
    when(col("salary") >= 5500, "A이상").otherwise("B이하").alias("grade"),
    coalesce(col("phone"), lit("UNKNOWN")).alias("phone_safe"),
)
# explain() 은 action 이 아니다 — 계획을 출력할 뿐 데이터를 읽지 않는다.
# extended=True 를 주면 Parsed / Analyzed / Optimized / Physical 4단계 모두 출력
final_query.explain(extended=False)
final_query.show(truncate=False)  # 실제 실행은 여기서


# =============================================================================
# ### Part 2 정답 모음 (spark-reviewer 채점용 — 학습자는 보지 마세요)
# =============================================================================

print("\n" + "=" * 60)
print("Part 2 정답 모음 (reviewer 참조용)")
print("=" * 60)

# ------------------------------------------------------------------
# Q1. 이메일 도메인 추출
# ------------------------------------------------------------------
print("\n[정답 Q1] 이메일 도메인 추출")
q1_data = spark.createDataFrame(
    [("alice@gmail.com",), ("bob@kakao.com",), ("carol@naver.com",)],
    schema=["email"],
)
q1_data.select(
    col("email"),
    split(col("email"), "@")[1].alias("domain"),
).show()

# ------------------------------------------------------------------
# Q2. 나이 → 연령대 매핑
# ------------------------------------------------------------------
print("\n[정답 Q2] 연령대 매핑")
q2_data = spark.createDataFrame(
    [(1, 17), (2, 25), (3, 33), (4, 45), (5, 62)],
    schema=["id", "age"],
)
q2_data.select(
    col("id"),
    col("age"),
    when(col("age") < 20, "10대")
    .when(col("age") < 30, "20대")
    .when(col("age") < 40, "30대")
    .when(col("age") < 50, "40대")
    .otherwise("50대 이상")
    .alias("age_group"),
).show()

# ------------------------------------------------------------------
# Q3. 할인가 계산
# ------------------------------------------------------------------
print("\n[정답 Q3] 할인가 계산")
q3_data = spark.createDataFrame(
    [(1, 29900, 0.10), (2, 59000, 0.25), (3, 12500, 0.05)],
    schema=["product_id", "price", "discount_rate"],
)
q3_data.select(
    col("product_id"),
    col("price"),
    col("discount_rate"),
    spark_round(col("price") * (lit(1.0) - col("discount_rate")), 2).alias("discounted_price"),
).show()

# ------------------------------------------------------------------
# Q4. NULL phone → 'UNKNOWN'
# ------------------------------------------------------------------
print("\n[정답 Q4] NULL 처리")
q4_data = spark.createDataFrame(
    [(1, "010-1111-2222"), (2, None), (3, "010-3333-4444"), (4, None)],
    schema=["id", "phone"],
)
q4_data.select(
    col("id"),
    col("phone"),
    coalesce(col("phone"), lit("UNKNOWN")).alias("phone_safe"),
).show()

# ------------------------------------------------------------------
# Q5. 이름 정규화 — trim + initcap
# ------------------------------------------------------------------
print("\n[정답 Q5] 이름 정규화")
q5_data = spark.createDataFrame(
    [(1, "  kim minjun  "), (2, "LEE SEOYEON"), (3, " park jihoon")],
    schema=["id", "raw_name"],
)
q5_data.select(
    col("id"),
    col("raw_name"),
    initcap(trim(col("raw_name"))).alias("normalized_name"),
).show()

# =============================================================================

print("\n" + "=" * 60)
print("Lesson 02 Demo 완료!")
print("localhost:4040 → SQL/DataFrame 탭에서 각 쿼리 실행 계획을 확인하세요.")
print("특히 Step 10 의 explain() 출력과 UI 의 'Optimized Plan' 을 비교해 보세요.")
print("=" * 60)

spark.stop()
