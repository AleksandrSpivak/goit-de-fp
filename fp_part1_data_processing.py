# %%
import os
from pyspark.sql import SparkSession

from pyspark.sql.functions import col, from_json, to_json, struct, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DoubleType



# %%
kafka_input_topic = "AS_athlete_event_results"
kafka_output_topic = "AS_FP_part1_out"

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
# Конфігурація MySQL
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
jdbc_driver = "com.mysql.cj.jdbc.Driver"
jdbc_table_bio = "athlete_bio"
jdbc_table_results = "athlete_event_results"


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
schema = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("timestamp", StringType(), True),
])

# %%
# Read from MySQL
def read_from_mysql(table, partition_column):
    return (
        spark.read.format("jdbc")
        .options(
            url=jdbc_url,
            driver=jdbc_driver,
            dbtable=table,
            user=jdbc_user,
            password=jdbc_password,
            partitionColumn=partition_column,
            lowerBound=1,
            upperBound=1000000,
            numPartitions=10
        )
        .load()
    )

# %%
# Write to Kafka
def write_to_kafka(df, topic):
    (df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
     .write
     .format("kafka")
     .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'])
     .option("kafka.security.protocol", kafka_config['security_protocol'])
     .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
     .option("kafka.sasl.jaas.config", kafka_config['sasl_jaas_config'])
     .option("topic", topic)
     .save())
    


# %%
# Read and process data
df_event_results = read_from_mysql("athlete_event_results", "result_id")

# %%
write_to_kafka(df_event_results, kafka_input_topic)

# %%
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'])
    .option("kafka.security.protocol", kafka_config['security_protocol'])
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
    .option("kafka.sasl.jaas.config", kafka_config['sasl_jaas_config'])
    .option("subscribe", kafka_input_topic)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 5000)  # Обмеження кількості повідомлень за тригер
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)


# %%

df_athlete_bio_filtered = read_from_mysql("athlete_bio", "athlete_id").filter(
    (col("height").isNotNull() & (col("height") != "")) & 
    (col("weight").isNotNull() & (col("weight") != ""))
)

# Додавання префіксів до колонок у біологічному датафреймі
df_athlete_bio_filtered = df_athlete_bio_filtered.select(
    *[col(c).alias(f"bio_{c}") if c != "athlete_id" else col(c) for c in df_athlete_bio_filtered.columns]
)


# %%
aggregated_df = (
    kafka_df.join(df_athlete_bio_filtered, "athlete_id")
    .groupBy("sport", "medal", "bio_sex", "bio_country_noc")
    .agg(
        avg("bio_height").alias("avg_height"),
        avg("bio_weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )
)

# %%

jdbc_url_table_out = "jdbc:mysql://217.61.57.46:3306/aspivak"

# %%

def foreach_batch_function(batch_df, epoch_id):
    write_to_kafka(batch_df, kafka_output_topic)

    (batch_df.write
     .format("jdbc")
     .options(
         url=jdbc_url_table_out,
         driver=jdbc_driver,
         dbtable="medal_count",
         user=jdbc_user,
         password=jdbc_password
     )
     .mode("append")
     .save())



# %%
(aggregated_df.writeStream
 .foreachBatch(foreach_batch_function)
 .outputMode("update")
 .start()
 .awaitTermination())



