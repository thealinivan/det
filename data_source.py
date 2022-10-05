# Data source
NETFLIX_04_10_2022 = "data/netflix_titles.csv"
AMAZON_05_10_2022 = "data/amazon_prime_titles.csv"
DISNEY_05_10_2022 = "data/disney_plus_titles.csv"

# Streaming service provider name
AMAZON = "Amazon"
DISNEY = "Disney"
NETFLIX = "Netflix"

# Output file
PARQUET_FILE = "data/cleaned_films.parquet"

def getDataSources():
    return [\
        [NETFLIX_04_10_2022, NETFLIX],\
        [AMAZON_05_10_2022, AMAZON],\
        [DISNEY_05_10_2022, DISNEY]\
    ]

def getParquetFile():
    return PARQUET_FILE