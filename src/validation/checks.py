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
    exists in the given bronze DataFrame.

    Args:
        df: The bronze DataFrame to check (must have 'article' and
            'timestamp' columns).
        spark: Active SparkSession, used to build the expected set.
        article_titles: The full list of articles expected in bronze.
        start_date: Start date in YYYYMMDD format.
        end_date: End date in YYYYMMDD format.

    Returns:
        A dict with check_name, status ("PASS" or "WARN"),
        missing_count, and the missing rows themselves.
    """

    # Step 1: Figure out every date we EXPECT to have data for.
    # We're given a start date and an end date as plain text
    # (like "20260101"), so first we turn them into real Python
    # date objects we can actually do math with.
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    # Now we walk forward one day at a time, from start to end,
    # and build a list of every single date in between.
    # Example: if start is Jan 1 and end is Jan 3, this list
    # ends up as ["2026010100", "2026010200", "2026010300"].
    # We format each date to match the exact timestamp format
    # already used in the bronze table, so they can be compared later.
    expected_timestamps = []
    current = start
    while current <= end:
        expected_timestamps.append(current.strftime("%Y%m%d00"))
        current += timedelta(days=1)  # move forward one day and repeat

    # Step 2: Build the full list of things we EXPECT to see.
    # We expect every article to have a row for every date.
    # So we need every possible combination of (article, date).
    # This is exactly like a multiplication table: if you have
    # 10 articles and 31 dates, you expect 10 x 31 = 310 combinations.

    # First, turn our plain Python list of article names into
    # a tiny Spark table with one column called "article".
    articles_df = spark.createDataFrame(
        [(a,) for a in article_titles], ["article"]
    )

    # Do the same thing for our list of expected dates.
    dates_df = spark.createDataFrame(
        [(t,) for t in expected_timestamps], ["timestamp"]
    )

    # A "cross join" pairs up EVERY row from the first table with
    # EVERY row from the second table. This gives us one row for
    # every possible (article, date) combination — the complete
    # "should exist" list.
    expected_df = articles_df.crossJoin(dates_df)

    # Step 3: Look at what we ACTUALLY have in bronze.
    # We only care about which (article, date) pairs exist —
    # not how many times, not what the view count was — so we
    # pull just those two columns and remove any duplicates.
    actual_df = df.select("article", "timestamp").distinct()

    # Step 4: Compare "what we expect" against "what we actually have".
    # A "left_anti" join keeps ONLY the rows from expected_df that
    # have NO matching row in actual_df. In plain terms:
    # "Give me everything I expected to see, that I did NOT get."
    missing_df = expected_df.join(
        actual_df, on=["article", "timestamp"], how="left_anti"
    )

    # Count how many (article, date) pairs are missing.
    missing_count = missing_df.count()

    # Step 5: Report back a clear, simple result.
    # If nothing is missing, the check passes.
    # If anything is missing, we flag it as a warning — not a
    # hard failure, since missing data isn't automatically "bad"
    # data, just something worth knowing about.
    return {
        "check_name": "source_coverage",
        "status": "PASS" if missing_count == 0 else "WARN",
        "missing_count": missing_count,
        "missing_records": missing_df,
    }
