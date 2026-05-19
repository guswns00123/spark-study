# =============================================================================
# Lesson 01 — 연습 파일 (직접 채워보세요!)
# =============================================================================
#
# 아래 TODO 를 하나씩 완성한 뒤 spark-submit 으로 실행해 보세요.
# 막히는 부분은 lesson_01_demo.py 를 참고해도 됩니다.
#
# 실행 명령 (PowerShell):
#   docker exec spark-master spark-submit `
#       --master spark://spark-master:7077 `
#       /opt/bitnami/spark/examples/local/lesson_01_practice.py
#
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# =============================================================================
# TODO 1: SparkSession 만들기
# =============================================================================
# 힌트: SparkSession.builder.appName(...).getOrCreate()
# appName 은 "Lesson01_Practice" 로 설정하세요.
#
# 아래 ___ 부분을 채우세요.

spark = SparkSession.builder.appName('Lesson01_Practice').getOrCreate()  # TODO 1: SparkSession 생성

spark.sparkContext.setLogLevel("WARN")

print("SparkSession 이름:", spark.sparkContext.appName)

# =============================================================================
# TODO 2: 스키마 정의하기
# =============================================================================
# 아래 상품 데이터에 맞는 스키마를 작성하세요.
#
# 컬럼 목록:
#   product_id  : 정수형 (IntegerType),  null 불허
#   name        : 문자열 (StringType),   null 불허
#   category    : 문자열 (StringType),   null 불허
#   price       : 실수형 (DoubleType),   null 불허
#   stock       : 정수형 (IntegerType),  null 허용  ← 재고 미입력 가능
#
# 힌트: StructType([StructField("컬럼명", 타입(), nullable=True/False), ...])

product_schema = StructType([
    StructField("product_id",IntegerType(), nullable=False),
    StructField("name",StringType(), nullable=False),
    StructField("category",StringType(), nullable=False),
    StructField("price",DoubleType(), nullable=False),
    StructField("stock",IntegerType(), nullable=True),
                            ])  # TODO 2: 스키마 작성

# =============================================================================
# TODO 3: createDataFrame 으로 데이터 만들기
# =============================================================================
# 아래 데이터를 product_schema 를 사용해 DataFrame 으로 만드세요.
# 힌트: spark.createDataFrame(data=[...], schema=product_schema)

raw_data = [
    (1, "무선 이어폰",   "전자기기", 89000.0, 120),
    (2, "텀블러",       "생활용품", 25000.0, 45),
    (3, "노트북 거치대", "전자기기", 37500.0, None),   # 재고 미입력
    (4, "볼펜 세트",    "문구",     8900.0,  200),
    (5, "USB 허브",     "전자기기", 19800.0, 67),
]

products = spark.createDataFrame(data=raw_data, schema=product_schema)  # TODO 3: DataFrame 생성

# =============================================================================
# TODO 4: printSchema() 로 스키마 출력
# =============================================================================
# 힌트: DataFrame.printSchema()

print("\n[TODO 4] products 의 스키마:")
products.printSchema()

# =============================================================================
# TODO 5: show() 로 전체 데이터 출력 (truncate=False)
# =============================================================================
# 힌트: DataFrame.show(truncate=False)

print("\n[TODO 5] products 전체 데이터:")  # TODO 5: 데이터 출력
products.show(truncate=False)

# =============================================================================
# TODO 6: count() 로 행 수를 세어 출력
# =============================================================================
# 기대 출력: "상품 수: 5"
# 힌트: DataFrame.count()

total = products.count()  # TODO 6: 행 수 세기
print(f"\n[TODO 6] 상품 수: {total}")

# =============================================================================
# TODO 7: filter() 로 '전자기기' 카테고리만 골라서 show()
# =============================================================================
# 힌트: DataFrame.filter(DataFrame["컬럼명"] == "값")
# 기대 출력: 무선 이어폰, 노트북 거치대, USB 허브 3행

print("\n[TODO 7] 전자기기 카테고리만:")
electronics = products.filter(products["category"] == "전자기기")  # TODO 7-a: filter transformation
electronics.show(truncate=False)                 # TODO 7-b: show() action

# =============================================================================
# TODO 8 (도전!): spark.range() 로 1~20 숫자 시퀀스 만들기
# =============================================================================
# 조건: 파티션 4개, 결과를 show() 로 출력
# 힌트: spark.range(start, end, step, numPartitions)
#       → start=1, end=21 이면 1~20 이 나온다.
#       → spark.range(1, 21, numPartitions=4)

print("\n[TODO 8] 1~20 숫자 시퀀스 (파티션 4개):")
nums = spark.range(1,21,numPartitions=4)   # TODO 8-a: range 생성
nums.show()      # TODO 8-b: show()
print(f"파티션 수: {nums.rdd.getNumPartitions()}")  # 4 가 나오면 정답!

# =============================================================================
# [모범 답안 체크리스트]
# 실행 후 다음 항목을 스스로 확인해 보세요.
# =============================================================================
#
#  [ ] TODO 1: SparkSession 이 정상 생성되고 App Name 이 출력됐다.
#  [ ] TODO 2: printSchema() 에서 stock 컬럼만 nullable=true 로 표시됐다.
#  [ ] TODO 3: createDataFrame 이 에러 없이 실행됐다.
#  [ ] TODO 4: 5개 컬럼, 올바른 타입이 출력됐다.
#  [ ] TODO 5: 5행 전체가 잘려 나오지 않고 출력됐다.
#  [ ] TODO 6: "상품 수: 5" 가 출력됐다.
#  [ ] TODO 7: 전자기기 3행만 출력됐다.
#  [ ] TODO 8: 1~20 숫자가 보이고 파티션 수 = 4 가 출력됐다.
#
# localhost:4040 → Jobs 탭 → 몇 개의 Job 이 생겼는지 세어보세요.
# (힌트: action 호출 횟수만큼 Job 이 만들어집니다)
#
# =============================================================================

print("\n연습 완료! localhost:4040 에서 Job 개수를 확인하세요.")

spark.stop()
