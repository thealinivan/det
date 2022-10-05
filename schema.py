from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

# Input columns names
id = "show_id"
type = "type"
title = "title"
director = "director"
cast = "cast"
country = "country"
date_added = "date_added"
release_year = "release_year"
rating = "rating"
duration = "duration"
streaming_service = "streaming_service"

def getSchema():
    return StructType([\
        StructField(id, StringType(), True),\
        StructField(type, StringType(), True),\
        StructField(title, StringType(), True),\
        StructField(director, StringType(), True),\
        StructField(cast, StringType(), True),\
        StructField(country, StringType(), True),\
        StructField(date_added, StringType(), True),\
        StructField(release_year, IntegerType(), True),\
        StructField(rating, StringType(), True),\
        StructField(duration, StringType(), True)    
    ])