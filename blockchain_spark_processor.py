import os
import sys

# 1. Setup Environment
# Explicitly pointing to the Hadoop folder we fixed
os.environ['HADOOP_HOME'] = "D:\\blockchain_project\\hadoop"
sys.path.append("D:\\blockchain_project\\hadoop\\bin")
os.environ['PATH'] += os.pathsep + "D:\\blockchain_project\\hadoop\\bin"

# Java Home (Ensure this matches your installation)
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# 2. Initialize Spark with Kafka AND PostgreSQL Drivers
# Added 'org.postgresql:postgresql:42.7.3' to download the JDBC driver automatically
spark = SparkSession.builder \
    .appName("BlockchainSecurityETL") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
    .config("spark.sql.shuffle.partitions", "2") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 3. Define Schema (Matches the Producer data structure)
schema = StructType([
    StructField("tx_hash", StringType(), True),
    StructField("sender", StringType(), True),
    StructField("receiver", StringType(), True),
    StructField("value_eth", DoubleType(), True),
    StructField("gas", LongType(), True),
    StructField("gas_price", LongType(), True),
    StructField("nonce", LongType(), True),
    StructField("block_num", LongType(), True),
    StructField("timestamp", LongType(), True)
])

# 4. Read from Kafka (Bronze Layer)
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_transactions") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Transform & Enrich (Silver Layer)
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Security Logic: Tag high-value or suspicious transactions
enriched_df = parsed_df.withColumn(
    "security_status",
    when(col("value_eth") > 50, "WHALE_MOVEMENT")
    .when((col("value_eth") > 5) & (col("gas_price") > 100000000000), "HIGH_PRIORITY_ATTACK_RISK")
    .otherwise("NORMAL")
)

# 6. Write to PostgreSQL (Gold Layer)
# Function to write each micro-batch to the database
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/blockchain_gold") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "transactions") \
        .option("user", "admin") \
        .option("password", "my_secure_password") \
        .mode("append") \
        .save()

print("--- Pipeline Started: Writing Data to PostgreSQL ---")

# Start the streaming query
query = enriched_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "./spark_checkpoints_db") \
    .start()

query.awaitTermination()