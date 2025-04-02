from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, concat_ws, collect_list, expr, split, first, row_number
from pyspark.sql.window import Window

def main(input_path, output_path):
    # Инициализация Spark
    spark = SparkSession.builder \
        .appName("CrimeStatistics") \
        .getOrCreate()

    # Загрузка данных
    crimes_df = spark.read.csv(f"{input_path}/crime.csv", header=True, inferSchema=True)
    offense_codes_df = spark.read.csv(f"{input_path}/offense_codes.csv", header=True, inferSchema=True)

    # Очистка данных
    crimes_df = crimes_df.dropDuplicates()
    offense_codes_df = offense_codes_df.dropDuplicates(["CODE"])

    # Преобразование данных
    # Вычисляем crime_type как первую часть NAME из offense_codes
    offense_codes_df = offense_codes_df.withColumn(
        "crime_type",
        split(col("NAME"), "-").getItem(0)
    )

    # Объединяем таблицы по offense_code
    joined_df = crimes_df.join(offense_codes_df, crimes_df.OFFENSE_CODE == offense_codes_df.CODE, "inner")

    # Фильтрация и подготовка данных
    joined_df = joined_df.filter(col("DISTRICT").isNotNull() & col("Lat").isNotNull() & col("Long").isNotNull())

    # Агрегация данных
    window_spec = Window.partitionBy("DISTRICT").orderBy(col("crime_count").desc())
    aggregated_df = joined_df.groupBy("DISTRICT").agg(
        count("*").alias("crimes_total"),
        expr("percentile_approx(Lat, 0.5)").alias("lat"),
        expr("percentile_approx(Long, 0.5)").alias("lng"),
        avg("Lat").alias("avg_lat"),
        avg("Long").alias("avg_lng")
    )

    # Расчет медианы числа преступлений в месяц
    crimes_by_month_df = joined_df.withColumn(
        "year_month",
        concat_ws("-", col("YEAR"), col("MONTH"))
    ).groupBy("DISTRICT", "year_month").agg(count("*").alias("monthly_crimes"))

    median_crimes_df = crimes_by_month_df.groupBy("DISTRICT").agg(
        expr("percentile_approx(monthly_crimes, 0.5)").alias("crimes_monthly")
    )

    # Расчет трех самых частых типов преступлений
    frequent_crimes_df = joined_df.groupBy("DISTRICT", "crime_type").agg(
        count("*").alias("crime_count")
    ).withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(col("rank") <= 3).groupBy("DISTRICT").agg(
        concat_ws(", ", collect_list("crime_type")).alias("frequent_crime_types")
    )

    # Объединение всех метрик
    final_df = aggregated_df.join(median_crimes_df, "DISTRICT", "inner") \
        .join(frequent_crimes_df, "DISTRICT", "inner")

    # Сохранение результата
    final_df.write.parquet(output_path, mode="overwrite")

    # Остановка Spark
    spark.stop()

if __name__ == "__main__":
    import sys
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
    # to run:
    #spark-submit --master local[*] home_task.py ./datasets ./output