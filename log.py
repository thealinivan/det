# Print data validation result statistics and expectations status
# Args: 
    # JSON: result (expectations validation result)
# Return: None
def print_validation_result(result):
    print(result.statistics)
    for i, r in enumerate(result.results):
        print(str(i+1)+". "+\
            str(r.expectation_config.expectation_type)+" - "+\
                str(r.expectation_config.kwargs)+" - "+\
                    str(r.success))