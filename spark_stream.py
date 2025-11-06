import time, logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

KEYSPACE = "spark_streams"
TABLE = "created_users"
CHECKPOINT = "/tmp/checkpoint_users_created"

schema = StructType([
    StructField("id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("address", StringType(), False),
    StructField("post_code", StringType(), False),
    StructField("email", StringType(), False),
    StructField("username", StringType(), False),
    StructField("dob", StringType(), False),
    StructField("registered_date", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("picture", StringType(), False),
])

def ensure_schema():
    cluster = Cluster(['cassandra_db'])
    session = cluster.connect()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
    """)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name  TEXT,
            gender     TEXT,
            address    TEXT,
            post_code  TEXT,
            email      TEXT,
            username   TEXT,
            dob        TEXT,
            registered_date TEXT,
            phone      TEXT,
            picture    TEXT
        );
    """)
    session.shutdown()
    cluster.shutdown()

def main():
    spark = (SparkSession.builder
             .appName("KafkaToCassandra")
             .config("spark.cassandra.connection.host", "cassandra_db")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    print("✅ Spark OK")

    ensure_schema()
    print("✅ Keyspace/Table OK")

    kafka_opts = {
        "kafka.bootstrap.servers": "broker:29092",
        "subscribe": "users_created",
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "kafka.group.id": f"spark-users-{int(time.time())}",
    }

    raw = (spark.readStream
           .format("kafka")
           .options(**kafka_opts)
           .load()
           .selectExpr("CAST(value AS STRING) AS json"))

    parsed = (raw
              .select(from_json(col("json"), schema).alias("data"))
              .select("data.*")
              .dropna(subset=["id"]))

    query = (parsed.writeStream
             .format("org.apache.spark.sql.cassandra")
             .option("keyspace", KEYSPACE)
             .option("table", TABLE)
             .option("checkpointLocation", CHECKPOINT)
             .outputMode("append")
             .start())

    print("▶️ Streaming started")
    query.awaitTermination()

if __name__ == "__main__":
    main()
