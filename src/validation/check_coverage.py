"""
Bronze Data Quality Check: Source Coverage

This module checks whether the Bronze table contains every expected
(article, date) combination for the requested reporting period.

The check compares:

    EXPECTED DATA
        ↓
    Every article × every expected date

against:

    ACTUAL DATA
        ↓
    Article/date combinations that exist in Bronze

The difference between those two sets represents missing source data.

Example:

    10 articles × 31 days = 310 expected records

If Bronze contains only 287 unique article/date combinations:

    310 expected
    - 287 actual
    = 23 missing

The current project treats missing source coverage as WARN rather than
FAIL because the source API may legitimately fail to provide some data.

This module is responsible only for checking coverage.

It does NOT:

    - Fetch data from the API.
    - Write data to Bronze.
    - Decide whether the entire pipeline should stop.
    - Write DQ results to the audit table.

The orchestration layer is responsible for those decisions.
"""


from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession


# ============================================================================
# Source Coverage Check
# ============================================================================

def check_coverage(
    df: DataFrame,
    spark: SparkSession,
    article_titles: list[str],
    start_date: str,
    end_date: str,
) -> dict:
    """
    Check whether every expected (article, timestamp) combination exists.

    The function creates the complete set of records that SHOULD exist and
    compares that set against the records that ACTUALLY exist in Bronze.

    Args:
        df:
            Bronze DataFrame being checked.

            Must contain:

                - article
                - timestamp

        spark:
            Active SparkSession used to create the expected article and date
            DataFrames.

        article_titles:
            Complete list of articles expected in Bronze.

        start_date:
            Start date in YYYYMMDD format.

        end_date:
            End date in YYYYMMDD format.

    Returns:
        dict:
            Data-quality result containing:

                check_name:
                    Name of the check.

                status:
                    "PASS" when no records are missing.
                    "WARN" when one or more records are missing.

                missing_count:
                    Number of expected article/date combinations that are
                    missing from Bronze.

                missing_records:
                    Spark DataFrame containing the missing combinations.
    """

    # ------------------------------------------------------------------------
    # Step 1: Generate every expected date
    # ------------------------------------------------------------------------
    #
    # The API timestamps are stored using the format:
    #
    #     YYYYMMDD00
    #
    # For example:
    #
    #     2026010100
    #     2026010200
    #     2026010300
    #
    # We first convert the input strings into datetime objects so Python can
    # safely iterate through the requested date range.
    # ------------------------------------------------------------------------

    start = datetime.strptime(
        start_date,
        "%Y%m%d",
    )

    end = datetime.strptime(
        end_date,
        "%Y%m%d",
    )


    # Create an empty list that will contain every expected timestamp.
    expected_timestamps: list[str] = []


    # Start at the requested beginning date.
    current = start


    # Continue generating dates until we reach the end date.
    #
    # <= is intentional because the end date is inclusive.
    #
    # Example:
    #
    #     START_DATE = 20260101
    #     END_DATE   = 20260103
    #
    # Produces:
    #
    #     2026010100
    #     2026010200
    #     2026010300
    #

    while current <= end:

        expected_timestamps.append(
            current.strftime("%Y%m%d00")
        )

        # Move forward exactly one day.
        current += timedelta(days=1)


    # ------------------------------------------------------------------------
    # Step 2: Build the expected article DataFrame
    # ------------------------------------------------------------------------
    #
    # Spark works with DataFrames, so we convert our Python list of expected
    # articles into a small Spark DataFrame.
    #
    # Example:
    #
    #     article
    #     -----------------------
    #     Minecraft
    #     Fortnite_(video_game)
    #     Among_Us_(video_game)
    # ------------------------------------------------------------------------

    articles_df = spark.createDataFrame(
        [
            (article,)
            for article in article_titles
        ],
        ["article"],
    )


    # ------------------------------------------------------------------------
    # Step 3: Build the expected date DataFrame
    # ------------------------------------------------------------------------
    #
    # We perform the same conversion for our expected timestamps.
    #
    # Example:
    #
    #     timestamp
    #     ----------------
    #     2026010100
    #     2026010200
    #     2026010300
    # ------------------------------------------------------------------------

    dates_df = spark.createDataFrame(
        [
            (timestamp,)
            for timestamp in expected_timestamps
        ],
        ["timestamp"],
    )


    # ------------------------------------------------------------------------
    # Step 4: Create the complete expected dataset
    # ------------------------------------------------------------------------
    #
    # CROSS JOIN creates every possible combination of:
    #
    #     article × timestamp
    #
    # If we have:
    #
    #     10 articles
    #     31 dates
    #
    # then:
    #
    #     10 × 31 = 310 expected combinations
    #
    # This gives us a complete "what should exist?" dataset.
    # ------------------------------------------------------------------------

    expected_df = articles_df.crossJoin(
        dates_df
    )


    # ------------------------------------------------------------------------
    # Step 5: Extract the actual article/date combinations from Bronze
    # ------------------------------------------------------------------------
    #
    # We only need the two columns that identify whether a record exists:
    #
    #     article
    #     timestamp
    #
    # DISTINCT is important because duplicate records should not affect
    # coverage.
    #
    # For example, if Bronze contains:
    #
    #     Minecraft | 2026010100
    #     Minecraft | 2026010100
    #
    # that still means the expected combination exists.
    #
    # Duplicate records are handled separately by check_duplicates().
    # ------------------------------------------------------------------------

    actual_df = (
        df
        .select(
            "article",
            "timestamp",
        )
        .distinct()
    )


    # ------------------------------------------------------------------------
    # Step 6: Find missing records
    # ------------------------------------------------------------------------
    #
    # This is the key Spark operation in this check.
    #
    # LEFT ANTI JOIN returns records from the LEFT DataFrame
    # that have NO matching record in the RIGHT DataFrame.
    #
    # In this case:
    #
    #     LEFT  = expected_df
    #     RIGHT = actual_df
    #
    # Therefore the result means:
    #
    #     "Show me everything we EXPECTED but did NOT receive."
    #
    # Example:
    #
    # expected:
    #
    #     Minecraft | 2026010100
    #     Minecraft | 2026010200
    #     Minecraft | 2026010300
    #
    # actual:
    #
    #     Minecraft | 2026010100
    #     Minecraft | 2026010300
    #
    # missing:
    #
    #     Minecraft | 2026010200
    # ------------------------------------------------------------------------

    missing_df = expected_df.join(
        actual_df,
        on=[
            "article",
            "timestamp",
        ],
        how="left_anti",
    )


    # ------------------------------------------------------------------------
    # Step 7: Count missing combinations
    # ------------------------------------------------------------------------
    #
    # count() triggers Spark execution and tells us how many expected
    # article/date combinations are missing.
    # ------------------------------------------------------------------------

    missing_count = missing_df.count()


    # ------------------------------------------------------------------------
    # Step 8: Determine the check status
    # ------------------------------------------------------------------------
    #
    # PASS:
    #     Every expected article/date combination exists.
    #
    # WARN:
    #     At least one expected combination is missing.
    #
    # We intentionally use WARN instead of FAIL here because missing source
    # data does not automatically mean our ingestion code is broken.
    #
    # For example, the Wikimedia API may fail to return data for an article
    # on a particular day.
    #
    # The orchestration layer decides whether WARN results are allowed to
    # continue downstream.
    # ------------------------------------------------------------------------

    status = (
        "PASS"
        if missing_count == 0
        else "WARN"
    )


    # ------------------------------------------------------------------------
    # Step 9: Return the DQ result
    # ------------------------------------------------------------------------
    #
    # The validation function returns the information needed by the
    # orchestration layer.
    #
    # The orchestration layer can then:
    #
    #     1. Add run metadata.
    #     2. Write the result to dq_check_results.
    #     3. Decide whether a FAIL should stop the pipeline.
    #
    # Keeping those responsibilities outside this function makes the check
    # reusable and easier to unit test.
    # ------------------------------------------------------------------------

    return {
        "check_name": "source_coverage",
        "status": status,
        "missing_count": missing_count,
        "missing_records": missing_df,
    }
