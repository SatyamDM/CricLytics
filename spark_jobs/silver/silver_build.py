from spark_jobs.spark_session import get_spark_session
from pyspark.sql.functions import (col, when,coalesce,lit,
                                   size,concat_ws,sum as spark_sum,
                                   lower,count, row_number)

from pyspark.sql.window import Window

if __name__ == "__main__":
    spark = get_spark_session()

    jdbc_url = "jdbc:postgresql://postgres:5432/ipl_db"




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
        col("super_over_flag"),
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
                    ).otherwise(0)) \
        .withColumn("super_over_flag",
                    when(
                        lower(col("super_over_flag")) == "true", True)
        .otherwise(False)
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

    # df_fact_delivery.show(100,truncate = False)

    # df_fact_delivery.write \
    #     .mode("overwrite") \
    #     .partitionBy("season") \
    #     .parquet("data/silver/fact_delivery")

    print("Fact Delivery Count:", df_fact_delivery.count())

    print("Total Runs:",
      df_fact_delivery.agg({"runs_total": "sum"}).collect()[0][0])

    print("Total Wickets:",
      df_fact_delivery.filter(col("is_wicket") == 1).count())

    df_bronze_match.printSchema()
    # Fact_match Preparation
    df_fact_match = df_bronze_match.select(
        col("match_id"),
        col("season"),
        col("venue"),
        col("match_date"),
        col("teams")[0].alias("team1"),
        col("teams")[1].alias("team2"),
        col("outcome.winner").alias("winner"),
        col("outcome.by.runs").alias("win_margin_runs"),
        col("outcome.by.wickets").alias("win_margin_wickets"),
        col("outcome.result").alias("result_type"),
        col("outcome.eliminator").alias("super_over_winner"),
        col("toss.winner").alias("toss_winner"),
        col("toss.decision").alias("toss_decision")
    )

    df_main = df_fact_delivery.filter(
        (col("super_over_flag").isNull()) |
        (col("super_over_flag") == False)
    )

    df_team_runs = df_main.groupBy(
        "match_id","batting_team"
    ).agg(
        spark_sum("runs_total").alias("team_runs")
    )

    df_team_wickets = df_main.groupBy(
        "match_id","batting_team"
    ).agg(
        spark_sum("is_wicket").alias("team_wickets")
    )

    df_team_score = df_team_runs.join(
        df_team_wickets,
        ["match_id","batting_team"]
    )

    df_team1_score = df_team_score.join(
        df_fact_match.select("match_id","team1"),
        "match_id"
    ).filter(col("batting_team") == col("team1")) \
    .select(
        "match_id",
    col("team_runs").alias("team1_runs"),
    col("team_wickets").alias("team1_wickets")
    )

    df_team2_score = df_team_score.join(
        df_fact_match.select("match_id","team2"),
        "match_id"
    ).filter(col("batting_team") == col("team2")) \
        .select(
        "match_id",
        col("team_runs").alias("team2_runs"),
        col("team_wickets").alias("team2_wickets")
    )

    df_fact_match = df_fact_match \
        .join(df_team1_score,"match_id","left") \
        .join(df_team2_score,"match_id","left")

    df_fact_match = df_fact_match \
    .withColumn(
        "team1_score",
        when(col("team1_runs").isNull(), lit("NA"))
        .otherwise(
            concat_ws("/",
                      col("team1_runs").cast("int"),
                      col("team1_wickets").cast("int"))
        )
    ) \
    .withColumn(
                "team2_score",
                when(col("team2_runs").isNull(), lit("NA"))
                .otherwise(
                    concat_ws("/",
                              col("team2_runs").cast("int"),
                              col("team2_wickets").cast("int"))
                )
            )

    df_fact_match = df_fact_match.withColumn(
        "is_super_over",
        when(col("super_over_winner").isNotNull(), 1).otherwise(0)
    )

    df_fact_match = df_fact_match.withColumn(
        "final_winner",
        when(col("is_super_over") == 1, col("super_over_winner"))
        .when(col("winner").isNotNull(), col("winner"))
        .when(col("result_type") == "tie", None)
        .when(col("result_type") == "no result", None)
        .when(col("team1_runs") > col("team2_runs"), col("team1"))
        .when(col("team2_runs") > col("team1_runs"), col("team2"))
    )

    df_fact_match = df_fact_match.withColumn(
        "win_type",
        when(col("is_super_over") == 1, "super_over")
        .when(col("win_margin_runs").isNotNull(), "runs")
        .when(col("win_margin_wickets").isNotNull(), "wickets")
        .when(col("team1_runs") == col("team2_runs"), "tie")
        .otherwise("derived")
    )

    df_fact_match = df_fact_match.withColumn(
        "result_string",
        when(col("win_type") == "runs",
             concat_ws(" ", col("final_winner"), lit("won by"),
                       col("win_margin_runs"), lit("runs")))
        .when(col("win_type") == "wickets",
              concat_ws(" ", col("final_winner"), lit("won by"),
                        col("win_margin_wickets"), lit("wickets")))
        .when(col("win_type") == "super_over",
              concat_ws(" ", col("final_winner"), lit("won in Super Over")))
        .when(col("win_type") == "tie", lit("Match Tied"))
        .otherwise(
            concat_ws(" ",
                      col("final_winner"),
                      lit("won (derived from score)"))
        )
    )

    df_dim_team = df_fact_match.select(
        col("team1").alias("team_name")
    ).union(
        df_fact_match.select(
            col("team2").alias("team_name")
    )).distinct()

    df_dim_team = df_dim_team.withColumn(
        "team_key",
        row_number().over(Window.orderBy("team_name"))
    )

    df_dim_players = df_fact_delivery.select(
        col("batter").alias("player_name")
    ).union(
        df_fact_delivery.select(col("bowler"))
    ).union(
        df_fact_delivery.select(col("non_striker"))
    ).distinct()

    df_dim_players = df_dim_players.withColumn(
    "player_key",
    row_number().over(Window.orderBy("player_name"))
    )

    df_dim_venue = df_fact_match.select("venue").distinct() \
        .withColumn("venue_key", row_number().over(Window.orderBy("venue")))

    df_dim_season = df_fact_match.select("season").distinct() \
        .withColumn("season_key", row_number().over(Window.orderBy("season")))

    df_fact_delivery = df_fact_delivery \
        .join(df_dim_team,df_fact_delivery.batting_team == df_dim_team.team_name, "left") \
        .drop("team_name") \
        .withColumnRenamed("team_key","batting_team_key")

    df_fact_delivery = df_fact_delivery \
        .join(df_dim_team,df_fact_delivery.bowling_team == df_dim_team.team_name, "left") \
        .drop("team_name") \
        .withColumnRenamed("team_key","bowling_team_key")

    df_fact_delivery = df_fact_delivery \
        .join(df_dim_players,df_fact_delivery.batter == df_dim_players.player_name, "left") \
        .drop("player_name") \
        .withColumnRenamed("player_key","batter_key")

    df_fact_delivery = df_fact_delivery \
        .join(df_dim_players,df_fact_delivery.bowler == df_dim_players.player_name, "left") \
        .drop("player_name") \
        .withColumnRenamed("player_key","bowler_key")

    df_fact_delivery = df_fact_delivery \
        .join(df_dim_players,df_fact_delivery.non_striker == df_dim_players.player_name, "left") \
        .drop("player_name") \
        .withColumnRenamed("player_key","non_striker_key")

    df_fact_delivery = df_fact_delivery \
        .join(df_dim_venue,df_fact_delivery.venue == df_dim_venue.venue, "left") \
        .drop("venue") \
        .withColumnRenamed("venue_key","venue_key")

    df_fact_delivery = df_fact_delivery \
        .join(df_dim_season,df_fact_delivery.season == df_dim_season.season, "left") \
        .drop("season") \
        .withColumnRenamed("season_key","season_key")

    df_fact_match = df_fact_match \
        .join(df_dim_venue,df_fact_match.venue == df_dim_venue.venue, "left") \
        .drop("venue") \
        .withColumnRenamed("venue_key","venue_key")

    df_fact_match = df_fact_match \
        .join(df_dim_season,df_fact_match.season == df_dim_season.season, "left") \
        .drop("season") \
        .withColumnRenamed("season_key","season_key")

    df_fact_match = df_fact_match \
        .join(df_dim_team,df_fact_match.team1 == df_dim_team.team_name, "left") \
        .withColumnRenamed("team_key","team1_key") \
        .drop("team1","team_name")

    df_fact_match = df_fact_match \
        .join(df_dim_team,df_fact_match.team2 == df_dim_team.team_name, "left") \
        .withColumnRenamed("team_key","team2_key") \
        .drop("team2","team_name")

    df_fact_match = df_fact_match \
        .join(df_dim_team,df_fact_match.winner == df_dim_team.team_name, "left") \
        .withColumnRenamed("team_key","winner_team_key") \
        .drop("winner","team_name")

    # Final Selection
    df_fact_delivery = df_fact_delivery.select(
        "match_id",
        "season_key",
        "venue_key",
        "inning_number",
        "batting_team_key",
        "bowling_team_key",
        "super_over_flag",
        "over_number",
        "ball_number",
        "ball_id",
        "batter_key",
        "bowler_key",
        "non_striker_key",
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

    df_fact_match = df_fact_match.select(
    "match_id",
    "season_key",
    "venue_key",
    "match_date",
    "team1_key",
    "team2_key",
    "winner_team_key",
    "win_margin_runs",
    "win_margin_wickets",
    "result_type",
    "super_over_winner",
    "toss_winner",
    "toss_decision",
    "team1_runs",
    "team1_wickets",
    "team2_runs",
    "team2_wickets",
    "team1_score",
    "team2_score",
    "is_super_over",
    "final_winner",
    "win_type",
    "result_string"
    )



    # df_fact_match.write \
    # .mode("overwrite") \
    # .partitionBy("season") \
    # .parquet("data/silver/fact_match")




    print("Total Rows:", df_fact_match.count())

    print("Unique Match IDs:",
      df_fact_match.select("match_id").distinct().count())

    print("Null Winners:",

    df_fact_match.filter(
          (col("result_type").isNull()) &
          (col("final_winner").isNull())
      ).count())



    df_fact_match.filter(col("final_winner").isNull()) \
        .select(
        "match_id",
        "team1_key",
        "team2_key",
        "team1_score",
        "team2_score",
        "result_type",
        "winner_team_key"
    ).show(truncate=False)




    (
    df_fact_delivery.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "fact_delivery")
    .option("user", "admin")
    .option("password", "admin")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
    )



    (
    df_fact_match.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "fact_match")
    .option("user", "admin")
    .option("password", "admin")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
    )

    (
        df_dim_team.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_team")
        .option("user", "admin")
        .option("password", "admin")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    (
        df_dim_players.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_players")
        .option("user", "admin")
        .option("password", "admin")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    (
        df_dim_venue.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_venue")
        .option("user", "admin")
        .option("password", "admin")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    (
        df_dim_season.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_season")
        .option("user", "admin")
        .option("password", "admin")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )



    #VALIDATION LAYER
    # 1. Duplicate Ball ID Check
    dup_ball = df_fact_delivery.groupBy("ball_id") \
        .agg(count("*").alias("cnt")) \
        .filter(col("cnt") > 1)

    if dup_ball.count() > 0:
        print("Duplicate ball_id found!")

    # 2. NULL Checks in Mandatory Cols
    critical_cols = ["match_id","batting_team","over_number","ball_number"]

    for c in critical_cols:
        null_count = df_fact_delivery.filter(col(c).isNull()).count()
        if null_count > 0:
            raise Exception(f"Nulls found in {c} : {null_count}")

    #  3. RUN consistency CHECK
    df_match_runs = df_fact_delivery.groupBy("match_id").agg(
       spark_sum("runs_total").alias("total_runs")
    )

    df_team_runs = df_fact_match.select(
        "match_id",
        (col("team1_runs") + col("team2_runs")).alias("expected_runs")
    )
    df_validation_runs = df_match_runs.join(df_team_runs,"match_id")

    mismatch_runs = df_validation_runs.filter(col("total_runs") != col("expected_runs"))

    if mismatch_runs.count() > 0:
        print("Run Mismatch Found !")
        mismatch_runs.show()

    #  4. WICKET consistency CHECK
    df_match_wickets = df_fact_delivery.groupBy("match_id").agg(
        spark_sum("is_wicket").alias("total_wickets")
    )

    df_team_wickets = df_fact_match.select(
        "match_id",
        (col("team1_wickets") + col("team2_wickets")).alias("expected_wickets")
    )
    df_validation_wickets = df_match_wickets.join(df_team_wickets,"match_id")

    mismatch_wickets = df_validation_wickets.filter(col("total_wickets") != col("expected_wickets"))

    if mismatch_wickets.count() > 0:
        print("Wickets Mismatch Found !")
        mismatch_wickets.show()

    # 5. DOMAIN CHECKS
    df_fact_delivery.filter(col("runs_batter") < 0).show()
    df_fact_delivery.filter(col("over_number") > 20).show()


spark.stop()