"""
Bronze Data Quality Check: Schema Validation

This module checks whether the Bronze DataFrame has the schema that the
pipeline expects.

The check validates two things:

    1. Column names
    2. Column data types

For example, if we expect:

    article     -> string
    timestamp   -> string
    views       -> bigint

but Bronze contains:

    article     -> string
    timestamp   -> string
    views       -> string

the check will identify the incorrect `views` data type.

Schema validation is important because downstream transformations depend
on the structure and data types of the Bronze data.

This module is responsible only for identifying schema mismatches.

It does NOT:

    - Modify the DataFrame.
    - Cast columns into the correct types.
    - Write DQ results to the audit table.
    - Decide whether the pipeline should stop.

The orchestration layer is responsible for handling the returned DQ result.
"""


from pyspark.sql import DataFrame


# ============================================================================
# Schema Validation
# ============================================================================

def check_schema(
    df: DataFrame,
    expected_schema: dict,
) -> dict:
    """
    Check whether a Bronze DataFrame matches the expected schema.

    The check verifies that:

        - Every expected column exists.
        - Every expected column has the correct data type.
        - No unexpected columns exist.

    Args:
        df:
            Bronze DataFrame being checked.

        expected_schema:
            Dictionary mapping column names to their expected Spark data type.

            Example:

                {
                    "project": "string",
                    "article": "string",
                    "granularity": "string",
                    "timestamp": "string",
                    "views": "bigint",
                }

    Returns:
        dict:
            Data-quality result containing:

                check_name:
                    Name of the check.

                status:
                    "PASS" when the schema matches exactly.
                    "WARN" when one or more mismatches are found.

                mismatches:
                    List describing missing columns, unexpected columns,
                    or incorrect data types.
    """

    # ------------------------------------------------------------------------
    # Step 1: Convert the Spark schema into a simple dictionary
    # ------------------------------------------------------------------------
    #
    # Spark provides the schema through df.schema.
    #
    # Each field contains information such as:
    #
    #     field.name
    #     field.dataType
    #
    # We convert that information into:
    #
    #     {
    #         "article": "string",
    #         "timestamp": "string",
    #         "views": "bigint"
    #     }
    #
    # simpleString() gives us an easy-to-compare representation of the Spark
    # data type.
    # ------------------------------------------------------------------------

    actual_schema = {
        field.name: field.dataType.simpleString()
        for field in df.schema
    }

    # ------------------------------------------------------------------------
    # Step 2: Create a collection for schema mismatches
    # ------------------------------------------------------------------------
    #
    # Every problem discovered during validation will be added to this list.
    #
    # Keeping all mismatches allows us to report every schema problem in one
    # DQ result instead of discovering and fixing them one at a time.
    # ------------------------------------------------------------------------

    mismatches: list[str] = []

    # ------------------------------------------------------------------------
    # Step 3: Check expected columns
    # ------------------------------------------------------------------------
    #
    # For every column that the pipeline expects, we check:
    #
    #     1. Does the column exist?
    #     2. If it exists, does it have the correct data type?
    #
    # This catches both missing columns and incorrect data types.
    # ------------------------------------------------------------------------

    for column, expected_type in expected_schema.items():

        # --------------------------------------------------------------------
        # Check whether the expected column exists
        # --------------------------------------------------------------------

        if column not in actual_schema:

            mismatches.append(
                f"Missing column: {column}"
            )

        # --------------------------------------------------------------------
        # Check whether the column has the expected data type
        # --------------------------------------------------------------------

        elif actual_schema[column] != expected_type:

            mismatches.append(
                f"Type mismatch on {column}: "
                f"expected {expected_type}, "
                f"got {actual_schema[column]}"
            )

    # ------------------------------------------------------------------------
    # Step 4: Check for unexpected columns
    # ------------------------------------------------------------------------
    #
    # The previous loop checks whether everything we EXPECTED exists.
    #
    # We also need to check the opposite direction:
    #
    #     "Did something appear that we weren't expecting?"
    #
    # Unexpected columns can indicate:
    #
    #     - An API response changed.
    #     - A transformation added a field unexpectedly.
    #     - An ingestion process changed.
    #     - A schema drift issue occurred.
    #
    # Detecting schema drift early prevents surprises downstream.
    # ------------------------------------------------------------------------

    for column in actual_schema:

        if column not in expected_schema:

            mismatches.append(
                f"Unexpected column: {column}"
            )

    # ------------------------------------------------------------------------
    # Step 5: Determine the DQ status
    # ------------------------------------------------------------------------
    #
    # PASS:
    #     No schema mismatches were found.
    #
    # WARN:
    #     At least one schema mismatch was detected.
    #
    # The validation function reports the problem. The orchestration layer
    # decides whether a schema mismatch should block the pipeline.
    # ------------------------------------------------------------------------

    status = (
        "PASS"
        if len(mismatches) == 0
        else "WARN"
    )

    # ------------------------------------------------------------------------
    # Step 6: Return the DQ result
    # ------------------------------------------------------------------------
    #
    # The orchestration layer can take this result and add:
    #
    #     - check_run_id
    #     - layer
    #     - table_name
    #     - checked_at
    #
    # before writing the result to the central DQ audit table.
    # ------------------------------------------------------------------------

    return {
        "check_name": "schema_check",
        "status": status,
        "mismatches": mismatches,
    }
