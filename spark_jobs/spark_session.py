from pyspark.sql import SparkSession

def get_spark_session():
    return (
        SparkSession.builder
        .appName("IPL Analytics")

        # 🔥 THIS IS THE FIX
        .config(
            "spark.jars",
            "/opt/jars/postgresql-42.6.0.jar"
        )

        .config(
            "spark.driver.extraClassPath",
            "/opt/jars/postgresql-42.6.0.jar"
        )

        .config(
            "spark.executor.extraClassPath",
            "/opt/jars/postgresql-42.6.0.jar"
        )

        .getOrCreate()
    )


if __name__ == "__main__":
    spark = get_spark_session()
    print("Spark Session Created Successfully!")
    spark.stop()
