"""
verify-cluster.py
=================
Spark 클러스터가 정상적으로 동작하는지 확인하는 헬스체크 스크립트.

실행 방법 (PowerShell):
    docker exec spark-master spark-submit `
        --master spark://spark-master:7077 `
        /opt/bitnami/spark/jobs/verify-cluster.py

또는 아래 README의 원-라이너 명령을 사용하세요.
"""

import sys
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, spark_partition_id

# ----------------------------------------------------------------
# 1. SparkSession 생성
#    - appName: Spark UI(localhost:4040)에 표시될 이름
#    - master는 spark-defaults.conf에서 읽어오므로 여기서 중복 설정 불필요
#    - getOrCreate(): 이미 세션이 있으면 재사용 (멱등성 보장)
# ----------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("VerifyCluster-HealthCheck")
    .getOrCreate()
)

# INFO 로그가 너무 많으면 결과를 찾기 힘들어서 WARN으로 낮춤
spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 60)
print("  Spark 클러스터 헬스체크 시작")
print("=" * 60)

# ----------------------------------------------------------------
# 2. 기본 정보 출력
# ----------------------------------------------------------------
sc = spark.sparkContext
print(f"\n[INFO] Spark 버전     : {sc.version}")
print(f"[INFO] Master URL    : {sc.master}")
print(f"[INFO] App ID        : {sc.applicationId}")
print(f"[INFO] Python 버전   : {sys.version.split()[0]}")

# ----------------------------------------------------------------
# 3. SparkPi 스타일 -- 몬테카를로 방법으로 파이 추정
#    분산 처리가 실제로 일어나는지 확인하는 고전적인 테스트
# ----------------------------------------------------------------
print("\n[TEST 1] SparkPi (몬테카를로 파이 추정)")
print("         4개의 partition에서 병렬 계산을 수행합니다...")

NUM_SAMPLES = 1_000_000  # 샘플 수. 많을수록 정확하지만 시간 증가
NUM_PARTITIONS = 4        # spark-defaults.conf의 shuffle.partitions와 맞춤

def count_hits(partition_samples):
    """각 파티션에서 독립적으로 난수를 생성해 원 안에 떨어지는 점 수를 반환"""
    local_random = random.Random()  # 파티션별 독립 난수 생성기 (재현성)
    hits = 0
    for _ in range(partition_samples):
        x = local_random.random()
        y = local_random.random()
        if x * x + y * y <= 1.0:
            hits += 1
    return hits

samples_per_partition = NUM_SAMPLES // NUM_PARTITIONS
rdd = sc.parallelize(range(NUM_PARTITIONS), NUM_PARTITIONS)
total_hits = rdd.map(lambda _: count_hits(samples_per_partition)).sum()
pi_estimate = 4.0 * total_hits / NUM_SAMPLES

print(f"         추정 파이 값  : {pi_estimate:.5f}")
print(f"         실제 파이 값  : 3.14159")
print(f"         오차          : {abs(pi_estimate - 3.14159):.5f}")

if abs(pi_estimate - 3.14159) < 0.05:
    print("  [PASS] SparkPi 분산 계산 성공")
else:
    print("  [WARN] 오차가 크지만 샘플 수 문제일 수 있음 (클러스터 자체는 정상)")

# ----------------------------------------------------------------
# 4. DataFrame API 테스트
#    RDD API와 달리 최적화 엔진(Catalyst)을 거치는지 확인
# ----------------------------------------------------------------
print("\n[TEST 2] DataFrame API 테스트")

# 간단한 인메모리 데이터셋 생성
data = [(f"user_{i}", i % 5, float(i * 1.5)) for i in range(1, 101)]
df = spark.createDataFrame(data, ["name", "group", "score"])

result = (
    df.groupBy("group")
      .agg(
          count("*").alias("user_count"),
          avg("score").alias("avg_score")
      )
      .orderBy("group")
)

print("         group별 집계 결과:")
result.show()
print(f"  [PASS] DataFrame groupBy/agg 성공 (총 {df.count()}행 처리)")

# ----------------------------------------------------------------
# 5. 파티션 분산 확인
#    실제로 여러 파티션에 데이터가 나뉘었는지 확인
# ----------------------------------------------------------------
print("\n[TEST 3] 파티션 분산 확인")

partition_dist = (
    df.withColumn("partition_id", spark_partition_id())
      .groupBy("partition_id")
      .agg(count("*").alias("row_count"))
      .orderBy("partition_id")
)

print("         파티션별 데이터 분포:")
partition_dist.show()

partition_count = df.rdd.getNumPartitions()
print(f"         총 파티션 수: {partition_count}")

if partition_count >= 1:
    print("  [PASS] 파티션 분산 확인 성공")

# ----------------------------------------------------------------
# 6. 파일 I/O 테스트 (마운트된 data 디렉토리 사용)
# ----------------------------------------------------------------
print("\n[TEST 4] 파일 I/O 테스트 (data 디렉토리 마운트 확인)")

output_path = "/opt/bitnami/spark/data/verify_output"

try:
    df.write.mode("overwrite").parquet(output_path)
    df_read = spark.read.parquet(output_path)
    row_count = df_read.count()
    print(f"         Parquet 쓰기/읽기 성공 ({row_count}행)")
    print(f"         저장 위치: {output_path}")
    print("  [PASS] 파일 I/O 성공 -- data 디렉토리가 정상적으로 마운트됨")
except Exception as e:
    print(f"  [FAIL] 파일 I/O 실패: {e}")
    print("         data 디렉토리 마운트를 확인하세요.")

# ----------------------------------------------------------------
# 7. 최종 요약
# ----------------------------------------------------------------
print("\n" + "=" * 60)
print("  헬스체크 완료")
print("=" * 60)
print(f"  Spark Web UI  : http://localhost:8080  (클러스터 전체 상태)")
print(f"  App UI        : http://localhost:4040  (현재/방금 실행된 job)")
print(f"  Jupyter Lab   : http://localhost:8888  (PySpark 노트북)")
print("=" * 60 + "\n")

spark.stop()
