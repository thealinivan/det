from pyspark.sql.functions import col, to_date, from_unixtime, unix_timestamp, lit, when
from data_source import getDataSources, getParquetFile
from spark_session import getSparkSession
from schema import getSchema, id, type, title, director, cast, country, date_added, release_year, rating, duration, streaming_service
from file_manipulation import readFromCSVFile, readFromParquetFile, writeToParquetFile

# Filters and Processing information
types_movie = "Movie"
rating_tv_ma = "TV-MA"
rating_r = "R"
countries_us = "United States"
date_added_structure = 'MMMM dd, yyyy'
date_added_expected_structure = 'yyyy-MM-dd'

# Additional columns names
airing_date = "airing_date"
streaming_service = "streaming_service"
adult = "adult"
american = "american"
runtime = duration

spark = getSparkSession()
  
# Process data frame
def processDataFrame(df, streaming_service_name):
    df = df.filter(col(type).contains(types_movie))                                                  # filter Movie type only
    df = df.withColumn(date_added, 
                to_date(from_unixtime(unix_timestamp(
                    col(date_added), date_added_structure), date_added_expected_structure)))         # cast to date
    df = df.withColumnRenamed(date_added, airing_date)                                               # column rename
    df = df.withColumn(streaming_service, lit(streaming_service_name))                               # constant value
    df = df.withColumn(adult, when(col(rating).contains(rating_tv_ma) | 
                col(rating).contains(rating_r), True).otherwise(False))                              # based on 2 different types of rating
    df = df.withColumn(american, when(col(country).contains(countries_us), True).otherwise(False))   # including US as country
    df = df.withColumn(american, when(col(country) == countries_us, True).otherwise(False))          # only US as country
    df.printSchema()                                                                                 # log schema structure
    cleaned_films = df.select(
        title, 
        director,
        airing_date,
        country,
        adult,
        american,
        streaming_service,
        runtime
    )                                                    
    return cleaned_films

def main():
    for provider in getDataSources():
        df = readFromCSVFile(provider[0], getSchema(), spark)
        cleaned_data = processDataFrame(df, provider[1])
        writeToParquetFile(cleaned_data, getParquetFile())    
    readFromParquetFile(getParquetFile(), spark).sort(title).show()                                            # log: cross validation of Parquet data                                                                      

main()
