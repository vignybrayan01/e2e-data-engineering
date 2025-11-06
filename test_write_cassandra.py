from pyspark.sql import SparkSession, Row

spark = (SparkSession.builder
    .appName("TestWriteCassandra")
    .config("spark.cassandra.connection.host","cassandra_db")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

rows = [Row(id="smoketest-1", first_name="Ana", last_name="Garcia", gender="female",
            address="Test", post_code="12345", email="ana@example.com", username="ana",
            dob="1968-09-23", registered_date="2021-11-03", phone="0000", picture="-")]
df = spark.createDataFrame(rows)

(df.write.format("org.apache.spark.sql.cassandra")
  .options(keyspace="spark_streams", table="created_users")
  .mode("append").save())

print("✅ WRITE OK")
spark.stop()
