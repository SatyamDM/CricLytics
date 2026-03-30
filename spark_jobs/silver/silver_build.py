from spark_jobs.spark_session import get_spark_session
from pyspark.sql.functions import col, when,coalesce,lit,size,concat_ws

if __name__ == "__main__":
    spark = get_spark_session()


    # Read Bronze tables
    df_bronze_delivery = spark.read.parquet("data/bronze/delivery")
    df_bronze_match = spark.read.parquet("data/bronze/match")

    # df_bronze_delivery.show(20)
    # df_bronze_match.show(20)

    print("Bronze Delivery Count:",df_bronze_delivery.count())
    print("Bronze Match Count:",df_bronze_match.count())

    df_bronze_match.printSchema()
    df_bronze_delivery.printSchema()

    # Fact_Delivery Preparation
    df_fact_delivery = df_bronze_delivery.select(
        col("match_id"),
        col("season"),
        col("venue"),
        col("inning_number"),
        col("batting_team"),
        col("over_number"),
        col("ball_number"),
        col("delivery.batter").alias("batter"),
        col("delivery.bowler").alias("bowler"),
        col("delivery.non_striker").alias("non_striker"),
        col("delivery.runs.batter").alias("runs_batter"),
        col("delivery.runs.extras").alias("runs_extras"),
        col("delivery.runs.total").alias("runs_total"),
        col("delivery.wickets").alias("wickets"),
        coalesce(col("delivery.extras.wides"), lit(0)).alias("wides"),
        coalesce(col("delivery.extras.legbyes"), lit(0)).alias("legbyes"),
        coalesce(col("delivery.extras.byes"), lit(0)).alias("byes"),
        coalesce(col("delivery.extras.noballs"), lit(0)).alias("noballs"),
            )

    df_fact_delivery = df_fact_delivery \
        .withColumn("is_four",
                    when(col("runs_batter") == 4, 1).otherwise(0)
                    ) \
        .withColumn("is_six",
                    when(col("runs_batter") == 6, 1).otherwise(0)
                    ) \
        .withColumn("is_dot",
                    when(col("runs_total") == 0, 1).otherwise(0)
                    ) \
        .withColumn("is_wicket",
                    when(size(col("wickets")) > 0,1).otherwise(0)
                    ) \
        .withColumn("dismissal_kind",
                    col("wickets")[0]["kind"]
                    ) \
        .withColumn("player_out",
                    col("wickets")[0]["player_out"]
                    ) \
        .withColumn("is_powerplay",
                    when(col("over_number") < 6,1).otherwise(0)
                    ) \
        .withColumn("is_death_over",
                    when(col("over_number") >= 16,1).otherwise(0)
                    ) \
        .withColumn("is_legal_ball",
                    when(
                        (col("wides") == 0) & (col("noballs") == 0),
                        1
                    ).otherwise(0)
                    )
    df_fact_delivery = df_fact_delivery.join(
        df_bronze_match.select("match_id","teams"),
        "match_id"
    )

    df_fact_delivery = df_fact_delivery.withColumn(
        "bowling_team",
        when(col("batting_team") == col("teams")[0]
             ,col("teams")[1])
        .otherwise(col("teams")[0])
    ) \
    .withColumn("ball_id",
                concat_ws("_",
                          col("match_id"),
                          col("inning_number"),
                          col("over_number"),
                          col("ball_number"),
                          )
                )
    # Final Selection
    df_fact_delivery = df_fact_delivery.select(
        "match_id",
        "season",
        "venue",
        "inning_number",
        "batting_team",
        "bowling_team",
        "over_number",
        "ball_number",
        "ball_id",
        "batter",
        "bowler",
        "non_striker",
        "runs_batter",
        "runs_extras",
        "runs_total",
        "wides",
        "legbyes",
        "byes",
        "noballs",
        "is_legal_ball",
        "is_wicket",
        "dismissal_kind",
        "player_out",
        "is_four",
        "is_six",
        "is_dot",
        "is_powerplay",
        "is_death_over"
    )
    df_fact_delivery.show(100,truncate = False)

    df_fact_delivery.write \
        .mode("overwrite") \
        .partitionBy("season") \
        .parquet("data/silver/fact_delivery")

    print("Fact Delivery Count:", df_fact_delivery.count())

    print("Total Runs:",
      df_fact_delivery.agg({"runs_total": "sum"}).collect()[0][0])

    print("Total Wickets:",
      df_fact_delivery.filter(col("is_wicket") == 1).count())

    spark.stop()