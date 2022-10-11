from platform import release
import great_expectations as ge
from schema import id, type, title, director, cast, country, date_added, release_year, rating, duration, streaming_service


# Create expectations
# Args:
    # string: csv_data_source (the relative file path for the CSV data source)
    # string: expec_file_path (the relative file path to save the expectations suite)
# Return: None
def create_expectations(csv_data_source, expec_file_path):
    train = ge.read_csv(csv_data_source)
    
    # Columns presence
    train.expect_column_to_exist(id)
    train.expect_column_to_exist(type)
    train.expect_column_to_exist(title)
    train.expect_column_to_exist(director)
    train.expect_column_to_exist(cast)
    train.expect_column_to_exist(country)
    train.expect_column_to_exist(date_added)
    train.expect_column_to_exist(release_year)
    train.expect_column_to_exist(rating)
    train.expect_column_to_exist(duration)

    train.expect_column_values_to_not_be_null(id)
    train.expect_column_values_to_be_unique(id)

    train.expect_column_distinct_values_to_be_in_set(type, {'Movie', 'TV Show'})

    train.expect_column_values_to_not_be_null(title)
    train.expect_table_row_count_to_be_between(0, 10000)

    train.expect_column_values_to_be_of_type(release_year, 'int')
    train.expect_column_values_to_be_between(release_year, 1900, 2022)
    #...

    train.save_expectation_suite(expec_file_path)

# Validate data for a given streaming service
# Args:
    # string: csv_data_source (the relative file path for the CSV data source)
    # string: expec_file_path (the relative file path to load the expectations suite)
# Return: 
    # JSON: result (the validation result)
def validate_data(csv_data_source, expec_file_path):
    test = ge.read_csv(csv_data_source)
    result = test.validate(expectation_suite=expec_file_path)
    if result["success"]:
        return result
    else:
        raise Exception("Data validation has failed..")