from pyspark.sql import DataFrame


def check_schema(df: DataFrame, expected_schema: dict) -> dict:
    """
    Check whether a Bronze DataFrame's schema matches what's
    expected — same column names, same data types.

    Args:
        df: The Bronze DataFrame to check.
        expected_schema: A dict mapping column name to expected
            data type as a string, e.g. {"views": "bigint"}.

    Returns:
        A dictionary containing:
            check_name: Name of the data quality check.
            status: "PASS" when the schema matches exactly,
                    otherwise "WARN".
            mismatches: A list describing any missing, extra,
                or wrong-type columns found.
    """

    # Build a simple dict of the DataFrame's actual columns and
    # their types, so we can compare it against what we expect.
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema}

    mismatches = []

    # Check for columns we expected but didn't find, or found
    # with the wrong type.
    for column, expected_type in expected_schema.items():
        if column not in actual_schema:
            mismatches.append(f"Missing column: {column}")
        elif actual_schema[column] != expected_type:
            mismatches.append(
                f"Type mismatch on {column}: expected {expected_type}, "
                f"got {actual_schema[column]}"
            )

    # Check for columns that showed up but weren't expected at all.
    for column in actual_schema:
        if column not in expected_schema:
            mismatches.append(f"Unexpected column: {column}")

    return {
        "check_name": "schema_check",
        "status": "PASS" if len(mismatches) == 0 else "WARN",
        "mismatches": mismatches,
    }
