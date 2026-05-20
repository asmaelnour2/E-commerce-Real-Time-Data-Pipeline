from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. الـ Schema الجميلة بتاعتك
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("device", StringType(), True),
    StructField("timestamp", StringType(), False)
])

# 2. بناء الـ Session
spark = SparkSession.builder \
    .appName("ClickstreamBatchProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n🚀 [1/4] Connecting to Kafka and fetching all available messages...")

# 3. القراءة كـ Batch (read وليس readStream) لتجنب استهلاك الموارد
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "clickstream") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

print("✅ [2/4] Data fetched successfully! Parsing JSON payloads...")

# 4. فك الـ JSON وتجهيز الداتا
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

# 5. التجميع الفوري والسريع
aggregated_df = parsed_df \
    .groupBy(col("event_type")) \
    .count() \
    .select(
        col("event_type"),
        col("count").alias("total_events")
    )

print("📊 Current Aggregation Preview:")
aggregated_df.show()

print("💾 [3/4] Writing directly to PostgreSQL...")

# 6. الصب المباشر والسريع في البوستجرس
aggregated_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/analytics_db") \
    .option("dbtable", "gold_windowed_metrics") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("🏆 [4/4] Pipeline finished successfully! Check your Postgres table now.")
spark.stop()