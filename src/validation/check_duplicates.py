from pyspark.sql import DataFrame


def check_duplicates(df: DataFrame) -> dict:
    """
    Check for duplicate (article, timestamp) combinations in
    a bronze DataFrame — each article should only have one row
    per day.

    Args:
        df: The bronze DataFrame to check.

    Returns:
        A dict with check_name, status ("PASS" or "WARN"),
        duplicate_count, and the duplicate rows themselves.
    """
    duplicate_counts = (
        df.groupBy("article", "timestamp")
          .count()
          .filter("count > 1")
    )

    duplicate_count = duplicate_counts.count()

    return {
        "check_name": "duplicate_check",
        "status": "PASS" if duplicate_count == 0 else "WARN",
        "duplicate_count": duplicate_count,
        "duplicate_records": duplicate_counts,
    }
