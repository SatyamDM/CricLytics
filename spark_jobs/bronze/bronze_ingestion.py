
from spark_jobs.spark_session import get_spark_session
from pyspark.sql.functions import (
    col,
    posexplode,
    input_file_name,
    regexp_extract,
    to_json
)

if __name__ == "__main__":
    spark = get_spark_session()

#     Read Raw JSON
    df = spark.read \
        .option("multiline", "true") \
        .option("mode", "PERMISSIVE") \
        .option("primitivesAsString", "true") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .json("data/raw/ipl_json")

#     Extract file path
    df = df.withColumn("file_path",input_file_name())

#     Extract match_id from file_path
    df = df.withColumn("match_id", regexp_extract(col("file_path"), r"([^/]+)\.json", 1))

#     Filter proper match_ids
    df = df.filter((col("match_id").isNotNull()) & (col("match_id") != ""))

#     Build Bronze_match
    bronze_match = df.select(
        col("match_id"),
        col("info.season").alias("season"),
        col("info.venue").alias("venue"),
        col("info.dates")[0].alias("match_date"),
        col("info.teams").alias("teams"),

        to_json(col("info.toss")).alias("toss_json"),
        to_json(col("info.outcome")).alias("outcome_json"),
        to_json(col("info")).alias("info_json")
    )
#     Build Bronze_delivery
    df_innings = df.select("*",posexplode(col("innings")).alias("inning_number","inning"))
    df_overs = df_innings.select("*",posexplode(col("inning.overs")).alias("over_index","over_data"))
    df_deliveries = df_overs.select("*",posexplode(col("over_data.deliveries")).alias("ball_number","delivery"))

    bronze_delivery = df_deliveries.select(
        col("match_id"),
        col("info.season").alias("season"),
        col("info.venue").alias("venue"),

        col("inning_number"),
        col("inning.team").alias("batting_team"),
        col("inning.super_over").cast("string").alias("super_over_flag"),
        col("over_data.over").alias("over_number"),
        col("ball_number"),
        col("delivery")
    )

#     Write bronze tables
    bronze_match.write \
        .mode("overwrite") \
        .partitionBy("season") \
        .parquet("data/bronze/match")

    bronze_delivery.write \
        .mode("overwrite") \
        .partitionBy("season") \
        .parquet("data/bronze/delivery")

#     Validation of Bronze Ingestion
    print("Bronze match count:",bronze_match.count())
    print("Bronze delivery count:",bronze_delivery.count())

    spark.stop()




