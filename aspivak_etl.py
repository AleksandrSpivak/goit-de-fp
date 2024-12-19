from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, udf
from pyspark.sql.types import StringType
import os
import re

from prefect import flow, task
# from prefect.tasks.shell import ShellOperation

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, avg, current_timestamp
from pyspark.sql.types import StringType
import requests

from pyspark.sql import SparkSession
import requests
import os



@task
def process_landing_to_bronze(table):

    abs_path = 'C:/Users/a.spivak/Documents/Projects/9. DE/goit-de-fp'

    spark = SparkSession.builder.appName('LandingToBronze').getOrCreate()

    url = f"https://ftp.goit.study/neoversity/{table}.csv"
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        # Шлях до збереження файлу
        file_path = os.path.join(abs_path, f"{table}.csv")
        os.makedirs(abs_path, exist_ok=True)  # Переконайтеся, що папка існує
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"Файл {table}.csv успішно збережено в {abs_path}")
    else:
        raise Exception(f"Failed to download {table}. Status code: {response.status_code}")
    
    local_path = os.path.join(abs_path, f"{table}.csv")
    output_path = os.path.join(abs_path, "bronze", table)
    os.makedirs(output_path, exist_ok=True)  # Переконайтеся, що папка існує

    # Читання CSV файлу
    df = spark.read.csv(local_path, header=True, inferSchema=True)
    df.show()
    
    # Збереження у форматі Parquet
    df.write.mode("overwrite").parquet(output_path)
    print(f"Дані з {table}.csv збережено у Parquet у {output_path}")
    spark.stop()

@task
def process_bronze_to_silver(table):

    abs_path = 'C:/Users/a.spivak/Documents/Projects/9. DE/goit-de-fp'

    def clean_text(text):
        return re.sub(r'[^a-zA-Z0-9,.\\"\\\']', '', str(text))

    # Створення UDF для очищення тексту
    clean_text_udf = udf(clean_text, StringType())

    spark = SparkSession.builder.appName('BronzeToSilver').getOrCreate()
    
    # Вхідний та вихідний шляхи з урахуванням abs_path
    input_path = os.path.join(abs_path, "bronze", table)
    output_path = os.path.join(abs_path, "silver", table)
    os.makedirs(output_path, exist_ok=True)

    # Читання даних із Bronze
    df = spark.read.parquet(input_path)

    # Обробка текстових колонок
    for column in df.columns:
        if df.schema[column].dataType == StringType():
            df = df.withColumn(column, clean_text_udf(trim(lower(col(column)))))

    # Видалення дублікатів
    df = df.dropDuplicates()
    df.show()

    # Збереження у Silver
    df.write.mode("overwrite").parquet(output_path)
    print(f"Таблиця {table} успішно оброблена та збережена у {output_path}")
    spark.stop()



@task
def process_silver_to_gold():

    abs_path = 'C:/Users/a.spivak/Documents/Projects/9. DE/goit-de-fp'
    spark = SparkSession.builder.appName('SilverToGold').getOrCreate()

    # Використовуємо абсолютний шлях до проекту для файлів
    athlete_bio_df = spark.read.parquet(os.path.join(abs_path, "silver/athlete_bio"))
    athlete_event_results_df = spark.read.parquet(os.path.join(abs_path, "silver/athlete_event_results"))

    athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

    joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
            current_timestamp().alias("timestamp")
        )
    aggregated_df.show()

    # Виведення результатів у золоту папку
    output_path = os.path.join(abs_path, "gold/avg_stats")
    os.makedirs(output_path, exist_ok=True)
    aggregated_df.write.mode("overwrite").parquet(output_path)
    spark.stop()


@flow
def AS_etl_pipeline(name="AS_ETL_Pipeline", log_prints=True):

    tables = ["athlete_bio", "athlete_event_results"]
    silver_results = []

    for table in tables:
        bronze_result = process_landing_to_bronze(table)
        silver_result = process_bronze_to_silver(table, wait_for=[bronze_result])
        silver_results.append(silver_result)  # Зберігаємо результат

    process_silver_to_gold(wait_for=silver_results)

if __name__ == "__main__":
    AS_etl_pipeline()
