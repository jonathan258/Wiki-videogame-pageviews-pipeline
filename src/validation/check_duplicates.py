"""
Bronze Data Quality Check: Duplicate Records

This module checks whether the Bronze table contains duplicate
(article, timestamp) combinations.

For this project, the combination of:

    article + timestamp

acts as the expected business key for a daily pageview record.

That means we expect at most one record for each:

    article + day

Example:

    Minecraft | 2026010100
    Minecraft | 2026010200
    Minecraft | 2026010300

is valid.

But:

    Minecraft | 2026010100
    Minecraft | 2026010100

contains a duplicate business key.

This module is responsible only for identifying duplicates.

It does NOT:

    - Remove duplicate records.
    - Write DQ results to the audit table.
    - Decide whether the pipeline should stop.

The orchestration layer is responsible for handling the returned DQ result.
"""


from pyspark.sql import DataFrame


# ============================================================================
# Duplicate Check
# ============================================================================

def check_duplicates(
    df: DataFrame,
) -> dict:
    """
    Check for duplicate article/timestamp combinations.

    Each article should have at most one pageview record for each daily
    timestamp.

    Args:
        df:
            Bronze DataFrame being checked.

            Must contain:

                - article
                - timestamp

    Returns:
        dict:
            Data-quality result containing:

                check_name:
                    Name of the check.

                status:
                    "PASS" when no duplicate combinations exist.
                    "WARN" when one or more duplicates are found.

                duplicate_count:
                    Number of article/timestamp combinations that occur
                    more than once.

                duplicate_records:
                    Spark DataFrame containing the duplicate combinations
                    and their occurrence counts.
    """

    # ------------------------------------------------------------------------
    # Step 1: Group records by the expected business key
    # ------------------------------------------------------------------------
    #
    # For this dataset, one article should have only one record for each
    # timestamp.
    #
    # Therefore:
    #
    #     article + timestamp
    #
    # is the combination we use to identify duplicates.
    #
    # groupBy() collects records that have the same values for both columns.
    #
    # count() tells us how many times each combination appears.
    # ------------------------------------------------------------------------

    duplicate_counts = (
        df
        .groupBy(
            "article",
            "timestamp",
        )
        .count()
    )

    # ------------------------------------------------------------------------
    # Step 2: Keep only combinations that occur more than once
    # ------------------------------------------------------------------------
    #
    # A valid article/timestamp combination should occur exactly once.
    #
    # Therefore:
    #
    #     count = 1  → valid
    #     count > 1  → duplicate
    #
    # The filter removes all valid combinations and leaves only the
    # combinations that violate our uniqueness expectation.
    # ------------------------------------------------------------------------

    duplicate_counts = duplicate_counts.filter(
        "count > 1"
    )

    # ------------------------------------------------------------------------
    # Step 3: Count the duplicate combinations
    # ------------------------------------------------------------------------
    #
    # count() triggers Spark execution and tells us how many unique
    # article/timestamp combinations contain duplicates.
    #
    # Important:
    #
    # duplicate_count is the number of duplicate KEY combinations, not
    # necessarily the total number of duplicate rows.
    #
    # For example:
    #
    #     Minecraft | 2026010100 | count = 3
    #
    # represents:
    #
    #     duplicate_count = 1
    #
    # but there are 3 physical records associated with that key.
    # ------------------------------------------------------------------------

    duplicate_count = duplicate_counts.count()

    # ------------------------------------------------------------------------
    # Step 4: Determine the DQ status
    # ------------------------------------------------------------------------
    #
    # PASS:
    #     Every article/timestamp combination is unique.
    #
    # WARN:
    #     At least one combination occurs more than once.
    #
    # We return WARN here rather than immediately failing the Spark job.
    # The orchestration layer can decide whether duplicates should be a
    # blocking issue for downstream processing.
    # ------------------------------------------------------------------------

    status = (
        "PASS"
        if duplicate_count == 0
        else "WARN"
    )

    # ------------------------------------------------------------------------
    # Step 5: Return the DQ result
    # ------------------------------------------------------------------------
    #
    # Returning the duplicate DataFrame is useful because it allows the
    # caller to inspect exactly which article/timestamp combinations caused
    # the warning.
    #
    # The orchestration layer can later decide whether these records should
    # be quarantined, removed, or simply recorded for investigation.
    # ------------------------------------------------------------------------

    return {
        "check_name": "duplicate_check",
        "status": status,
        "duplicate_count": duplicate_count,
        "duplicate_records": duplicate_counts,
    }
