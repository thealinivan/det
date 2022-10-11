# Files manipulation information
parquet_write_mode = "append"

# CSV read
# Args: 
    # string: data_source (file path of the CSV file)
    # StructType: schema (Spark data frame schema)
    # SparkSession: spark_session (the current Spark session)
# Return: DataFrame: (the data frame build from the CSV file)
def readFromCSVFile(data_source, schema, spark_session):
    return spark_session.read.format("csv")\
        .option("header", "true")\
        .option("delimiter", ",")\
        .schema(schema)\
        .load(data_source)

# Parquet read
# Args: 
    # string: data_source (file path of the Parquet)
    # SparkSession: spark_session (the current Spark session)
# Return: 
    # DataFrame: (the data frame build from the Parquet file)
def readFromParquetFile(data_source, spark_session):
     return spark_session.read.parquet(data_source);

# Parquet write
# Args: 
    # DataFrame: data_frame (the data frame)
    # string: file_location (file path of the Parquet file)
# Return: None
def writeToParquetFile(data_frame, file_location):
    data_frame.write.mode(parquet_write_mode).parquet(file_location) 
