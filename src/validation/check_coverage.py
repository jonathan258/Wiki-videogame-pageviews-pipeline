from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession


def check_coverage(
    df: DataFrame,
    spark: SparkSession,
    article_titles: list[str],
    start_date: str,
    end_date: str,
) -> dict:
    """
    Check whether every expected (article, date) combination
    exists in the given Bronze DataFrame.

    Args:
        df: The Bronze DataFrame to check. Must contain
            'article' and 'timestamp' columns.
        spark: Active SparkSession used to build the expected set.
        article_titles: Full list of articles expected in Bronze.
        start_date: Start date in YYYYMMDD format.
        end_date: End date in YYYYMMDD format.

    Returns:
        A dictionary containing:
            check_name: Name of the data quality check.
            status: "PASS" when no records are missing,
                    otherwise "WARN".
            missing_count: Number of missing article/date combinations.
            missing_records: Spark DataFrame containing missing combinations.
    """

    # Step 1: Determine every date we EXPECT to have data for.
    #
    # The start_date and end_date are provided as strings
    # such as "20260101". Convert them into Python datetime
    # objects so we can iterate through the date range.

    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    # Build a list containing every expected timestamp.
    #
    # Example:
    # start_date = "20260101"
    # end_date   = "20260103"
    #
    # Results:
    # ["2026010100", "2026010200", "2026010300"]

    expected_timestamps = []

    current = start

    while current <= end:
        expected_timestamps.append(
            current.strftime("%Y%m%d00")
        )

        current += timedelta(days=1)

    # Step 2: Build the complete set of expected
    # (article, timestamp) combinations.
    #
    # If we have 10 articles and 31 dates:
    #
    # 10 articles × 31 dates = 310 expected records.

    articles_df = spark.createDataFrame(
        [(article,) for article in article_titles],
        ["article"],
    )

    dates_df = spark.createDataFrame(
        [(timestamp,) for timestamp in expected_timestamps],
        ["timestamp"],
    )

    # CROSS JOIN creates every possible article/date combination.

    expected_df = articles_df.crossJoin(dates_df)

    # Step 3: Identify the (article, timestamp) combinations
    # that actually exist in Bronze.
    #
    # DISTINCT ensures duplicate Bronze records don't affect
    # the coverage calculation.

    actual_df = (
        df.select("article", "timestamp")
        .distinct()
    )

    # Step 4: Compare expected records against actual records.
    #
    # LEFT ANTI JOIN returns records that exist in expected_df
    # but do NOT exist in actual_df.
    #
    # In other words:
    #
    # "Show me everything we expected but did not receive."

    missing_df = expected_df.join(
        actual_df,
        on=["article", "timestamp"],
        how="left_anti",
    )

    # Count the missing article/date combinations.

    missing_count = missing_df.count()

    # Step 5: Return the data quality result.
    #
    # PASS = complete source coverage.
    # WARN = one or more expected records are missing.
    #
    # Missing source records are treated as a warning rather
    # than a hard failure because missing source data does not
    # necessarily mean the Bronze ingestion itself failed.

    return {
        "check_name": "source_coverage",
        "status": "PASS" if missing_count == 0 else "WARN",
        "missing_count": missing_count,
        "missing_records": missing_df,
    }
