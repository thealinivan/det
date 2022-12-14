from pyspark.sql.functions import col, to_date, from_unixtime, unix_timestamp, lit, when
from data_source import getDataSources, getParquetFile
from spark_session import getSparkSession
from schema import getSchema, id, type, title, director, cast, country, date_added, release_year, rating, duration, streaming_service
from file_manipulation import readFromCSVFile, readFromParquetFile, writeToParquetFile
from validation import create_expectations, validate_data
from log import print_validation_result

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
# Args:
    # DataFrame: df (the input data frame based on the CSV file)
    # string: streaming_service_name (the streaming service name)
# Return 
    # DataFrame: cleaned_films (the output data frame based on processing and cleaning)
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

# Main function
# Args: N/A
# Return: None
def main():

    # For each streaming service
        # create expectations and validate CSV file
        # clean, extract and save required data aggregating from all streaming services
    for provider in getDataSources():
        print("Processing "+provider[1]+"..")
        
        # Create expectations
        films_expec_path = "expectations/"+provider[1]+"_expectations.json"
        create_expectations(provider[0], films_expec_path)
        
        # Run validations
        print("Validating data..")
        result = validate_data(provider[0], films_expec_path)
        print("Displaying data validation result..")
        print_validation_result(result)

        # Clean and extract required data
        df = readFromCSVFile(provider[0], getSchema(), spark)
        print("Displaying "+provider[1]+" schema..")
        df.printSchema() 
        print("Displaying "+provider[1]+" raw data..")
        df.show()
        cleaned_data = processDataFrame(df, provider[1])

        # Save to Parquet file
        writeToParquetFile(cleaned_data, getParquetFile())
        print("Displaying "+provider[1]+" cleaned data..")
        cleaned_data.show()
    
    # Display the output data   
    print("Displaying agregated data from sources.. "+str([x[1] for x in getDataSources()]))   
    readFromParquetFile(getParquetFile(), spark).sort(title).show()                                     # log: cross validation of Parquet data                                                                      

main()
