from pyspark.sql import SparkSession

session_location = "local[1]"
app_name = "Data-Engineering-Training"

def getSparkSession():
    spark = SparkSession.builder.master(session_location)\
                        .appName(app_name)\
                        .getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    return spark