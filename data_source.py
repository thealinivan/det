# Data source
NETFLIX_CSV_FILE_LOCATION = "data/netflix_titles.csv"
STREAMING_SERVICE_NETFLIX = "Netflix"

AMAZON_CSV_FILE_LOCATION = "data/amazon_prime_titles.csv"
STREAMING_SERVICE_AMAZON = "Amazon"

DISNEY_CSV_FILE_LOCATION = "data/disney_plus_titles.csv"
STREAMING_SERVICE_DISNEY = "Disney"

PARQUET_FILE = "data/cleaned_films.parquet"

def getDataSources():
    return [\
        [NETFLIX_CSV_FILE_LOCATION, STREAMING_SERVICE_NETFLIX],\
        [AMAZON_CSV_FILE_LOCATION, STREAMING_SERVICE_AMAZON],\
        [DISNEY_CSV_FILE_LOCATION, STREAMING_SERVICE_DISNEY]\
    ]

def getParquetFile():
    return PARQUET_FILE