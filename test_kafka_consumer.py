from pyspark.sql import SparkSession

# Création de la session Spark avec le connecteur Kafka
spark = SparkSession.builder \
    .appName("KafkaDebug") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Lecture continue depuis le topic Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "users_created") \
    .option("startingOffsets", "earliest") \
    .load()

# Affichage du contenu brut des messages
df.selectExpr("CAST(value AS STRING)").writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()
