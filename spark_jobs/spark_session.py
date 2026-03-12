from pyspark.sql import SparkSession

def get_spark_session():
    spark = SparkSession.builder \
           .appName("IPL Analytics Platform-Simple") \
           .master("local[*]") \
           .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark

if __name__ == "__main__":
    spark = get_spark_session()
    print("Spark Session Created Successfully!")
    spark.stop()
