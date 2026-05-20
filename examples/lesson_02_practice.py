# =============================================================================
# Lesson 02 — Practice: 컬럼 표현식 & 내장 함수
# =============================================================================
#
# [사용 방법]
#   1. demo.py 를 먼저 끝까지 읽고 컨테이너에서 실행해 결과를 확인하세요.
#   2. 이 파일의 TODO 를 직접 채워보세요.
#   3. 다 채웠으면 spark-submit 으로 실행해서 출력 결과를 확인하세요.
#   4. 완료 후 "spark-reviewer, Lesson 02 봐줘" 로 채점을 요청하세요.
#
# [실행 방법]
#   docker exec spark-master \
#       /opt/bitnami/spark/bin/spark-submit \
#       /opt/bitnami/spark/examples/local/lesson_02_practice.py
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
    .appName("Lesson02_Practice")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Lesson 02 Practice 시작")
print("=" * 60)

# =============================================================================
# Part 1. 기초 TODO — demo 핵심 패턴 복습
# =============================================================================
#
# 아래 employees DataFrame 을 사용합니다. 수정하지 마세요.
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

# -----------------------------------------------------------------------------
# TODO 1: col() 과 lit() 이해
# -----------------------------------------------------------------------------
# 아래 코드에서 TODO 부분을 채우세요.
# 목표: id, name, salary 세 컬럼만 선택하고, "KR" 이라는 상수 컬럼 country 를 추가하세요.
# 힌트: select() + col() + lit()
#
print("\n[TODO 1] id, name, salary 선택 + country='KR' 상수 컬럼 추가")
print("-" * 40)
todo1 = employees.select(
    # TODO: id, name, salary 를 col() 로 선택하고
    #       lit("KR") 로 country 컬럼을 추가하세요.
    # 예상 컬럼: id | name | salary | country
    # --- 여기에 작성하세요 ---
    col('id'),
    col('name'),
    col('salary'),
    lit("KR").alias("country")
)
todo1.show(truncate=False)

# -----------------------------------------------------------------------------
# TODO 2: alias() 로 컬럼 이름 바꾸기
# -----------------------------------------------------------------------------
# 목표: salary 를 "연봉_만원" 으로, dept 를 "부서" 로 이름을 바꿔서 출력하세요.
# 힌트: col().alias()
#
print("\n[TODO 2] 컬럼 이름 alias 변경")
print("-" * 40)
todo2 = employees.select(
    col("id"),
    # TODO: salary 를 "연봉_만원" 으로, dept 를 "부서" 로 alias 하세요.
    # --- 여기에 작성하세요 ---
    col('salary').alias('연봉_만원'),
    col('dept').alias('부서')
)
todo2.show(truncate=False)

# -----------------------------------------------------------------------------
# TODO 3: withColumn() 으로 파생 컬럼 추가
# -----------------------------------------------------------------------------
# 목표: employees 에 bonus 컬럼을 추가하세요.
#       bonus = salary * discount_rate 를 계산하고 소수점 없이 반올림하세요.
# 힌트: withColumn() + col() 산술 연산 + spark_round(expr, 0)
#
print("\n[TODO 3] withColumn 으로 bonus 컬럼 추가")
print("-" * 40)
todo3 = (
    employees
    # TODO: bonus = salary * discount_rate, 소수점 0자리로 반올림
    # --- 여기에 작성하세요 ---
    .withColumn('bonus', spark_round(col('salary') * col('discount_rate'),0))
)
todo3.select("id", "name", "salary", "discount_rate", "bonus").show(truncate=False)

# -----------------------------------------------------------------------------
# TODO 4: when / otherwise 조건부 컬럼
# -----------------------------------------------------------------------------
# 목표: 부서(dept) 에 따라 team_type 컬럼을 만드세요.
#       Engineering → "기술팀"
#       Marketing   → "영업팀"
#       그 외        → "기타팀"
# 힌트: when(col("dept") == "...", "...").when(...).otherwise(...)
#
print("\n[TODO 4] when/otherwise 로 team_type 컬럼 생성")
print("-" * 40)
todo4 = employees.withColumn(
    "team_type",
    # TODO: 위 조건으로 team_type 을 만드세요.
    # --- 여기에 작성하세요 ---
    when(col('dept') == 'Engineering', '기술팀')
    .when(col('dept') == 'Marketing','영업팀')
    .otherwise('기타팀')
)
todo4.select("id", "dept", "team_type").show(truncate=False)

# -----------------------------------------------------------------------------
# TODO 5: 날짜 함수 — 입사 연도와 근속 일수 계산
# -----------------------------------------------------------------------------
# 목표: hire_date 문자열을 DateType 으로 변환하고,
#       입사 연도(hire_year) 와 오늘까지 근속 일수(days_since_hire) 를 구하세요.
# 힌트: to_date(col, "yyyy-MM-dd"), year(), datediff(current_date(), date_col)
#
print("\n[TODO 5] 날짜 함수 — 입사 연도 + 근속 일수")
print("-" * 40)
todo5 = employees.select(
    col("id"),
    col("name"),
    col("hire_date"),
    # TODO: hire_year 컬럼과 days_since_hire 컬럼을 추가하세요.
    # --- 여기에 작성하세요 ---
    to_date(col("hire_date"), "yyyy-MM-dd").alias("hire_date"),  # 문자열 → DateType
    year(to_date(col("hire_date"), "yyyy-MM-dd")).alias("hire_year"),
    datediff(
        current_date(),                                        # 오늘 날짜
        to_date(col("hire_date"), "yyyy-MM-dd")
    ).alias("days_since_hire"),
    
)
todo5.show(truncate=False)

# -----------------------------------------------------------------------------
# TODO 6: 문자열 함수 — initcap + trim
# -----------------------------------------------------------------------------
# 목표: name 컬럼을 trim() 으로 앞뒤 공백을 제거하고,
#       initcap() 으로 각 단어 첫 글자를 대문자로 만들어
#       name_clean 컬럼으로 추가하세요.
# 힌트: initcap(trim(col("name")))
#
print("\n[TODO 6] 문자열 정규화 — trim + initcap")
print("-" * 40)
todo6 = employees.select(
    col("id"),
    col("name").alias("name_original"),
    # TODO: name_clean 컬럼을 추가하세요.
    # --- 여기에 작성하세요 ---
    initcap(trim(col("name"))).alias("name_clean"),
)
todo6.show(truncate=False)

# -----------------------------------------------------------------------------
# TODO 7: NULL 처리 — coalesce
# -----------------------------------------------------------------------------
# 목표: phone 컬럼이 null 인 행을 'UNKNOWN' 으로 채워
#       phone_safe 컬럼을 만드세요.
# 힌트: coalesce(col("phone"), lit("UNKNOWN"))
#
print("\n[TODO 7] NULL 처리 — coalesce")
print("-" * 40)
todo7 = employees.select(
    col("id"),
    col("phone"),
    # TODO: phone_safe 컬럼을 추가하세요.
    # --- 여기에 작성하세요 ---
    coalesce(col('phone'),lit('UNKNOWN')).alias('phone_safe')
)
todo7.show(truncate=False)


# =============================================================================
# Part 2. 응용 문제 — 코딩 테스트 / 면접 스타일
# =============================================================================
#
# 각 문제에는 입력 데이터가 미리 주어져 있습니다.
# TODO 블록을 채워 기대 출력과 같은 결과를 만드세요.
# 정답은 demo.py 의 "Part 2 정답 모음" 섹션에 있습니다.
#

# =============================================================================
# [Q1] 이메일 도메인 추출
# =============================================================================
#
# [시나리오]
#   사용자 DB 에서 이메일 주소만 있고 도메인 분류가 없다.
#   이메일 컬럼에서 "@" 뒤 문자열(도메인)만 추출해 domain 컬럼을 만들어라.
#
# [입력 데이터]
#   email
#   ─────────────────
#   alice@gmail.com
#   bob@kakao.com
#   carol@naver.com
#
# [기대 출력]
#   email            | domain
#   ─────────────────┼──────────
#   alice@gmail.com  | gmail.com
#   bob@kakao.com    | kakao.com
#   carol@naver.com  | naver.com
#
# [힌트] split(col, "@") 는 ArrayType 을 반환합니다. 인덱스로 특정 원소에 접근하세요.
#
print("\n[Q1] 이메일 도메인 추출")
print("-" * 40)
q1_data = spark.createDataFrame(
    [("alice@gmail.com",), ("bob@kakao.com",), ("carol@naver.com",)],
    schema=["email"],
)
q1_result = q1_data.select(
    col("email"),
    # TODO: domain 컬럼을 추가하세요.
    # --- 여기에 작성하세요 ---
    split(col("email"),'@')[1].alias('domain')
)
q1_result.show(truncate=False)

# =============================================================================
# [Q2] 나이 → 연령대 매핑
# =============================================================================
#
# [시나리오]
#   마케팅 팀이 연령대별 타겟팅을 원한다.
#   age 컬럼을 "10대 / 20대 / 30대 / 40대 / 50대 이상" 으로 분류해
#   age_group 컬럼을 만들어라.
#
# [입력 데이터]
#   id | age
#   ───┼────
#    1 |  17
#    2 |  25
#    3 |  33
#    4 |  45
#    5 |  62
#
# [기대 출력]
#   id | age | age_group
#   ───┼─────┼──────────
#    1 |  17 | 10대
#    2 |  25 | 20대
#    3 |  33 | 30대
#    4 |  45 | 40대
#    5 |  62 | 50대 이상
#
# [힌트] when(col("age") < 20, "10대").when(...).otherwise(...)
#
print("\n[Q2] 나이 → 연령대 매핑")
print("-" * 40)
q2_data = spark.createDataFrame(
    [(1, 17), (2, 25), (3, 33), (4, 45), (5, 62)],
    schema=["id", "age"],
)
q2_result = q2_data.select(
    col("id"),
    col("age"),
    # TODO: age_group 컬럼을 추가하세요. (when 체인 5단계)
    # --- 여기에 작성하세요 ---
    when(col("age") < 20, "10대")
    .when(col("age") < 30, "20대")
    .when(col("age") < 40, "30대")
    .when(col("age") < 50, "40대")
    .otherwise("50대 이상")
    .alias("age_group"),
)
q2_result.show()

# =============================================================================
# [Q3] 할인가 계산
# =============================================================================
#
# [시나리오]
#   쇼핑몰 DB 에 상품 가격과 할인율이 있다.
#   최종 할인가(discounted_price) 를 계산하고 소수점 둘째 자리까지 반올림하라.
#   공식: discounted_price = price * (1 - discount_rate)
#
# [입력 데이터]
#   product_id | price | discount_rate
#   ──────────┼───────┼──────────────
#            1 | 29900 |          0.10
#            2 | 59000 |          0.25
#            3 | 12500 |          0.05
#
# [기대 출력]
#   product_id | price | discount_rate | discounted_price
#   ──────────┼───────┼───────────────┼─────────────────
#            1 | 29900 |          0.10 |         26910.00
#            2 | 59000 |          0.25 |         44250.00
#            3 | 12500 |          0.05 |         11875.00
#
# [힌트] spark_round(col("price") * (lit(1.0) - col("discount_rate")), 2)
#        Python 상수 1.0 을 Column 과 계산할 때 lit() 이 필요합니다.
#
print("\n[Q3] 할인가 계산")
print("-" * 40)
q3_data = spark.createDataFrame(
    [(1, 29900, 0.10), (2, 59000, 0.25), (3, 12500, 0.05)],
    schema=["product_id", "price", "discount_rate"],
)
q3_result = q3_data.select(
    col("product_id"),
    col("price"),
    col("discount_rate"),
    # TODO: discounted_price 컬럼을 추가하세요. (소수점 2자리 반올림)
    # --- 여기에 작성하세요 ---
    spark_round(col("price") * (lit(1.0) - col("discount_rate")), 2).alias('discounted_price')
)
q3_result.show()

# =============================================================================
# [Q4] NULL phone → 'UNKNOWN' + 등급 조합
# =============================================================================
#
# [시나리오]
#   고객 테이블에 phone 이 없는 경우가 있다.
#   phone 이 null 이면 'UNKNOWN' 으로 채우고(phone_safe),
#   추가로 phone 이 있는지 여부에 따라 contact_status 를
#   '연락 가능' / '연락 불가' 로 분류하라.
#
# [입력 데이터]
#   id | phone
#   ───┼────────────────
#    1 | 010-1111-2222
#    2 | null
#    3 | 010-3333-4444
#    4 | null
#
# [기대 출력]
#   id | phone         | phone_safe    | contact_status
#   ───┼───────────────┼───────────────┼───────────────
#    1 | 010-1111-2222 | 010-1111-2222 | 연락 가능
#    2 | null          | UNKNOWN       | 연락 불가
#    3 | 010-3333-4444 | 010-3333-4444 | 연락 가능
#    4 | null          | UNKNOWN       | 연락 불가
#
# [힌트] coalesce 로 phone_safe, when(col("phone").isNull(), ...) 로 contact_status
#
print("\n[Q4] NULL 처리 + 상태 분류")
print("-" * 40)
q4_data = spark.createDataFrame(
    [(1, "010-1111-2222"), (2, None), (3, "010-3333-4444"), (4, None)],
    schema=["id", "phone"],
)
q4_result = q4_data.select(
    col("id"),
    col("phone"),
    # TODO: phone_safe 컬럼과 contact_status 컬럼을 추가하세요.
    # --- 여기에 작성하세요 ---
    coalesce(col('phone'),lit("UNKNOWN")).alias('phone_safe'),
    when(col('phone').isNull(), lit('연락 불가')).
    otherwise(lit('연락 가능')).alias('contact_status')
)
q4_result.show(truncate=False)

# =============================================================================
# [Q5] 이름 정규화 (면접 단골)
# =============================================================================
#
# [시나리오]
#   레거시 DB 에 이름이 불규칙하게 입력돼 있다.
#   앞뒤 공백을 제거하고(trim), 각 단어 첫 글자만 대문자로 변환해(initcap)
#   normalized_name 컬럼으로 만들어라.
#
# [입력 데이터]
#   id | raw_name
#   ───┼──────────────────
#    1 |   kim minjun
#    2 | LEE SEOYEON
#    3 |  park jihoon
#
# [기대 출력]
#   id | raw_name         | normalized_name
#   ───┼──────────────────┼─────────────────
#    1 |   kim minjun     | Kim Minjun
#    2 | LEE SEOYEON      | Lee Seoyeon
#    3 |  park jihoon     | Park Jihoon
#
# [힌트] initcap(trim(col("raw_name"))) — 두 함수를 중첩해서 쓰세요.
#        initcap 이 upper/lower 와 다른 점: 각 단어 첫 글자만 대문자.
#
print("\n[Q5] 이름 정규화 — trim + initcap")
print("-" * 40)
q5_data = spark.createDataFrame(
    [(1, "  kim minjun  "), (2, "LEE SEOYEON"), (3, " park jihoon")],
    schema=["id", "raw_name"],
)
q5_result = q5_data.select(
    col("id"),
    col("raw_name"),
    # TODO: normalized_name 컬럼을 추가하세요.
    # --- 여기에 작성하세요 ---
    initcap(trim(col('raw_name'))).alias('normalized_name')
)
q5_result.show(truncate=False)

# =============================================================================

print("\n" + "=" * 60)
print("Practice 완료!")
print("결과가 기대 출력과 같으면 'spark-reviewer, Lesson 02 봐줘' 로 채점 요청하세요.")
print("=" * 60)

spark.stop()
