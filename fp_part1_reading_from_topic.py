import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, from_json, to_json, struct, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DoubleType
from kafka import KafkaProducer
import json
from pyspark.sql import DataFrame


# %%

kafka_output_topic = "AS_FP_part1_out"

# %%
# Initialize Spark session
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1," \
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)
    
spark = (
    SparkSession.builder
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("JDBCToKafkaProcessing")
    .master("local[*]")
    .getOrCreate()
)
    

# %%
kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_jaas_config": "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';"
}

# %%
schema = StructType([
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("bio_sex", StringType(), True),
    StructField("bio_country_noc", StringType(), True),
    StructField("avg_height", DoubleType(), True),
    StructField("avg_weight", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

# %%

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'])
    .option("kafka.security.protocol", kafka_config['security_protocol'])
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
    .option("kafka.sasl.jaas.config", kafka_config['sasl_jaas_config'])
    .option("subscribe", kafka_output_topic)
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# %%
# Виведення даних на консоль
(kafka_df.writeStream
    .outputMode("append")  # Можна використовувати 'append', 'complete' або 'update'
    .format("console")     # Виведення на консоль
    .option("truncate", "false")  # Повний вивід без обрізки
    .option("numRows", 5)  # Кількість рядків для виведення
    .start()
    .awaitTermination())   # Чекати на завершення
