"""
Bronze Data Quality Runner

This module runs all Bronze-layer data quality checks against the
real Bronze table stored in Unity Catalog.

This code is intended to run inside Databricks because it requires
a live Spark session connected to Unity Catalog.

The workflow is:

    1. Load the Bronze table.
    2. Run each registered data quality check.
    3. Convert the results into a consistent DQ result format.
    4. Store the results in the DQ results Delta table.
    5. Fail the job if a hard FAIL is detected.

WARN results are recorded but do not stop the pipeline.
FAIL results are recorded and stop downstream processing.
"""

import uuid
from datetime import datetime, timezone

from validation import check_coverage, check_duplicates


# ============================================================================
# Configuration
# ============================================================================
#
# These values describe what data we expect to find in Bronze.
#
# Eventually, this configuration should be moved into a shared configuration
# file so that ingestion and data-quality checks do not maintain separate
# copies of the same information.
# ============================================================================

ARTICLE_TITLES = [
    "The_Legend_of_Zelda",
    "Super_Mario_Bros.",
    "Minecraft",
    "Fortnite_(video_game)",
    "Grand_Theft_Auto_V",
    "Call_of_Duty:_Modern_Warfare_(2019_video_game)",
    "Among_Us_(video_game)",
    "Cyberpunk_2077",
    "The_Witcher_3:_Wild_Hunt",
    "Red_Dead_Redemption_2",
]

# The Bronze coverage check uses these dates to determine whether every
# expected article/date combination was received.
START_DATE = "20260101"
END_DATE = "20260131"


# Unity Catalog location of the Bronze table.
CATALOG = "wiki_videogame_ingestion"
SCHEMA = "bronze"
TABLE = "wiki_videogame_pageviews"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"


# All data-quality results are stored in this Delta table.
#
# Keeping the results in a table gives us an audit history instead of only
# printing the results to the Databricks notebook.
DQ_RESULTS_TABLE = f"{CATALOG}.{SCHEMA}.dq_check_results"


# ============================================================================
# Bronze Data Quality Orchestration
# ============================================================================

def run_bronze_dq_checks(spark) -> str:
    """
    Run all Bronze-layer data quality checks.

    The function is responsible for orchestrating the checks. The individual
    validation modules are responsible for determining whether their specific
    check passes, warns, or fails.

    After all checks complete:

        - Results are written to the DQ results Delta table.
        - WARN results are recorded but allow the pipeline to continue.
        - FAIL results cause the job to fail and prevent downstream processing.

    Args:
        spark:
            Active SparkSession provided by Databricks.

    Returns:
        str:
            Unique check_run_id identifying this complete DQ execution.
    """

    # ------------------------------------------------------------------------
    # Create a unique ID for this DQ execution.
    #
    # Every check performed during this run receives the same ID. This allows
    # us to query all results belonging to one DQ run later.
    # ------------------------------------------------------------------------

    check_run_id = str(uuid.uuid4())

    # Use UTC so that DQ timestamps are consistent regardless of where the
    # Databricks job is running.
    checked_at = datetime.now(timezone.utc)

    # ------------------------------------------------------------------------
    # Load Bronze data
    # ------------------------------------------------------------------------
    #
    # The DQ checks should run against the actual Bronze table that was
    # produced by ingestion, rather than against a local file or test data.
    # ------------------------------------------------------------------------

    bronze_df = spark.table(BRONZE_TABLE)

    # ------------------------------------------------------------------------
    # Run Bronze data quality checks
    # ------------------------------------------------------------------------
    #
    # Each validation function is responsible for performing one specific
    # type of check and returning its result.
    #
    # Keeping these checks separate makes the code easier to test and allows
    # us to add additional checks later without putting all of the validation
    # logic into this orchestration file.
    # ------------------------------------------------------------------------

    # Check whether every expected article/date combination exists.
    coverage_result = check_coverage(
        df=bronze_df,
        spark=spark,
        article_titles=ARTICLE_TITLES,
        start_date=START_DATE,
        end_date=END_DATE,
    )

    # Check whether duplicate article/date records exist.
    duplicate_result = check_duplicates(
        df=bronze_df
    )

    # ------------------------------------------------------------------------
    # Display the Bronze schema
    # ------------------------------------------------------------------------
    #
    # This is useful during development and debugging because it lets us
    # confirm that the Bronze table contains the expected columns and data
    # types.
    #
    # This can eventually be replaced by a dedicated schema DQ check.
    # ------------------------------------------------------------------------

    print("Bronze table schema:")

    for field in bronze_df.schema:
        print(
            f"{field.name}: "
            f"{field.dataType.simpleString()}"
        )

    # ------------------------------------------------------------------------
    # Display individual check results
    # ------------------------------------------------------------------------
    #
    # These messages make the Databricks Job output easier to understand when
    # looking at a particular run.
    # ------------------------------------------------------------------------

    print(
        f"{coverage_result['check_name']}: "
        f"{coverage_result['status']} | "
        f"missing_count={coverage_result['missing_count']}"
    )

    print(
        f"{duplicate_result['check_name']}: "
        f"{duplicate_result['status']} | "
        f"duplicate_count={duplicate_result['duplicate_count']}"
    )

    # ------------------------------------------------------------------------
    # Normalize DQ results
    # ------------------------------------------------------------------------
    #
    # Each validation function can return information specific to its check.
    # Before storing the results, we convert them into one consistent schema.
    #
    # This makes dq_check_results easier to query because every check uses
    # the same columns:
    #
    #   check_run_id
    #   layer
    #   table_name
    #   check_name
    #   check_status
    #   expected_value
    #   actual_value
    #   details
    #   checked_at
    #
    # The shared run ID and timestamp are added here rather than inside each
    # validation function. This keeps the validation functions focused only
    # on checking the data.
    # ------------------------------------------------------------------------

    dq_rows = [
        {
            "check_run_id": check_run_id,
            "layer": "bronze",
            "table_name": BRONZE_TABLE,
            "check_name": coverage_result["check_name"],
            "check_status": coverage_result["status"],
            "expected_value": None,
            "actual_value": str(
                coverage_result["missing_count"]
            ),
            "details": None,
            "checked_at": checked_at,
        },
        {
            "check_run_id": check_run_id,
            "layer": "bronze",
            "table_name": BRONZE_TABLE,
            "check_name": duplicate_result["check_name"],
            "check_status": duplicate_result["status"],
            "expected_value": None,
            "actual_value": str(
                duplicate_result["duplicate_count"]
            ),
            "details": None,
            "checked_at": checked_at,
        },
    ]

    # ------------------------------------------------------------------------
    # Persist DQ results
    # ------------------------------------------------------------------------
    #
    # Write the results to a Delta table before checking for failures.
    #
    # This is intentional:
    #
    # If the job fails because of a DQ failure, we still want the failed
    # result recorded in the audit table so we can investigate what happened.
    # ------------------------------------------------------------------------

    dq_results_df = spark.createDataFrame(dq_rows)

    (
        dq_results_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(DQ_RESULTS_TABLE)
    )

    print(
        f"Bronze DQ results written to: "
        f"{DQ_RESULTS_TABLE}"
    )

    # ------------------------------------------------------------------------
    # Determine whether the pipeline should stop
    # ------------------------------------------------------------------------
    #
    # WARN:
    #     The data has an issue, but the pipeline is allowed to continue.
    #
    # FAIL:
    #     The data-quality requirement was violated. Raise an exception so
    #     the Databricks Job fails and downstream Silver processing does not
    #     run.
    #
    # For example, our current partial January coverage situation can be
    # represented as WARN, while duplicate records may be configured as FAIL.
    # ------------------------------------------------------------------------

    failures = [
        row
        for row in dq_rows
        if row["check_status"] == "FAIL"
    ]

    if failures:

        failed_checks = ", ".join(
            failure["check_name"]
            for failure in failures
        )

        raise ValueError(
            "Bronze DQ check(s) failed: "
            f"{failed_checks} "
            f"(check_run_id={check_run_id})"
        )

    # ------------------------------------------------------------------------
    # Successful completion
    # ------------------------------------------------------------------------
    #
    # If execution reaches this point, there were no hard FAIL results.
    # WARN results may still exist, but they have been recorded and are not
    # blocking the pipeline.
    # ------------------------------------------------------------------------

    print(
        "Bronze DQ checks completed successfully. "
        f"check_run_id={check_run_id}"
    )

    return check_run_id
