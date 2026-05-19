"""
io-tmp-test.py
==============
컨테이너 내부 /tmp 경로에 Parquet을 쓰고 읽어서 I/O 자체가 동작하는지 확인.
이게 성공하면 호스트 마운트 권한/OneDrive 동기화 문제로 좁혀진다.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("io-tmp-test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

data = [(i, f"name_{i}") for i in range(20)]
df = spark.createDataFrame(data, ["id", "name"])

# 컨테이너 내부 경로 (호스트 마운트 아님)
out = "/tmp/verify_output"

df.write.mode("overwrite").parquet(out)
print(f"WROTE -> {out}")

cnt = spark.read.parquet(out).count()
print(f"READ  <- {out} ({cnt} rows)")
print("RESULT: 컨테이너 내부 I/O 정상 작동" if cnt == 20 else "RESULT: 실패")

spark.stop()
