# Files manipulation information
parquet_write_mode = "append"

# Read
def readFromCSVFile(data_source, schema, spark_session):
    return spark_session.read.format("csv")\
        .option("header", "true")\
        .option("delimiter", ",")\
        .schema(schema)\
        .load(data_source)

def readFromParquetFile(data_source, spark_session):
     return spark_session.read.parquet(data_source);

# Write
def writeToParquetFile(data, file_location):
    data.write.mode(parquet_write_mode).parquet(file_location) 
