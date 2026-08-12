"""
Bronze Data Quality Runner

This module orchestrates all Bronze-layer data quality checks against the
real Bronze table stored in Unity Catalog.

This file is intended to run inside Databricks because it requires a live
Spark session connected to Unity Catalog.

The workflow is:

    1. Load the Bronze table.
    2. Run the registered data quality checks.
    3. Normalize the results into one consistent structure.
    4. Write the results to the DQ results Delta table.
    5. Fail the Databricks Job if a hard FAIL is detected.

WARN results are recorded but do not stop the pipeline.

FAIL results are recorded and stop downstream processing.

The individual validation modules are responsible for checking the data.

This orchestration module is responsible for deciding what to do with
those results.
"""


# ============================================================================
# Imports
# ============================================================================

import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession


# ============================================================================
# Project Imports
# ============================================================================

# The validation package lives inside the project's src directory.
#
# This path adjustment allows the Databricks notebook/job to find the
# validation modules when the repository is not installed as a Python package.
#
# Eventually, once the project is packaged properly, this can be removed
# and the imports can use the installed package directly.

sys.path.append(
    os.path.abspath("../src")
)

from validation import (
    check_coverage,
    check_duplicates,
    check_schema,
)


# ============================================================================
# Configuration
# ============================================================================

# ---------------------------------------------------------------------------
# Articles expected in the Bronze table
# ---------------------------------------------------------------------------
#
# These should match the article list used by the ingestion pipeline.
#
# Keeping this list here works for the current project, but eventually we
# should move shared configuration into one central configuration file so
# ingestion and DQ checks cannot accidentally use different lists.
# ---------------------------------------------------------------------------

ARTICLE_TITLES = [
    "The_Legend_of_Zelda",
    "Super_Mario_Bros.",
    "Minecraft",
    "Fortnite_(video_game)",
    "Grand_Theft_Auto_V",
    "Call_of_Duty:*Modern_Warfare*(2019_video_game)",
    "Among_Us_(video_game)",
    "Cyberpunk_2077",
    "The_Witcher_3:_Wild_Hunt",
    "Red_Dead_Redemption_2",
]


# ---------------------------------------------------------------------------
# Expected coverage period
# ---------------------------------------------------------------------------
#
# The coverage check uses these dates to determine whether every expected
# article/date combination exists in Bronze.
#
# Both dates are inclusive.
#
# January 1 through January 31:
#
#     10 articles × 31 days = 310 expected combinations
# ---------------------------------------------------------------------------

START_DATE = "20260101"
END_DATE = "20260131"


# ---------------------------------------------------------------------------
# Expected Bronze schema
# ---------------------------------------------------------------------------
#
# These are the columns and Spark data types we expect to find in the
# Bronze table.
#
# This allows the schema DQ check to detect:
#
#     - Missing columns
#     - Incorrect data types
#     - Unexpected columns
# ---------------------------------------------------------------------------

EXPECTED_SCHEMA = {
    "article": "string",
    "granularity": "string",
    "project": "string",
    "timestamp": "string",
    "views": "bigint",
    "_ingested_at": "timestamp",
    "_ingestion_job_run_id": "string",
}


# ---------------------------------------------------------------------------
# Unity Catalog Bronze table
# ---------------------------------------------------------------------------

CATALOG = "wiki_videogame_ingestion"
SCHEMA = "bronze"
TABLE = "wiki_videogame_pageviews"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"


# ---------------------------------------------------------------------------
# DQ results table
# ---------------------------------------------------------------------------
#
# All DQ results are stored in a Delta table.
#
# Keeping the results in a table gives us an audit history instead of
# relying only on Databricks Job logs.
# ---------------------------------------------------------------------------

DQ_RESULTS_TABLE = (
    f"{CATALOG}.{SCHEMA}.dq_check_results"
)


# ============================================================================
# Bronze Data Quality Orchestration
# ============================================================================

def run_bronze_dq_checks(
    spark: SparkSession,
) -> str:
    """
    Run all Bronze-layer data quality checks.

    The function:

        1. Creates a unique DQ run ID.
        2. Loads the Bronze table.
        3. Runs coverage, duplicate, and schema checks.
        4. Normalizes their results.
        5. Writes the results to the DQ audit table.
        6. Raises an exception if a hard FAIL exists.

    Args:
        spark:
            Active SparkSession provided by Databricks.

    Returns:
        str:
            Unique check_run_id identifying this complete DQ execution.
    """

    # ------------------------------------------------------------------------
    # Step 1: Create a unique ID for this DQ execution
    # ------------------------------------------------------------------------
    #
    # Every check performed during this execution receives the same ID.
    #
    # This allows us to query all results belonging to one DQ run later.
    #
    # Example:
    #
    #     check_run_id = "550e8400-e29b-41d4-a716-446655440000"
    # ------------------------------------------------------------------------

    check_run_id = str(
        uuid.uuid4()
    )


    # ------------------------------------------------------------------------
    # Step 2: Record the execution timestamp
    # ------------------------------------------------------------------------
    #
    # UTC is used so timestamps remain consistent regardless of where the
    # Databricks Job happens to execute.
    # ------------------------------------------------------------------------

    checked_at = datetime.now(
        timezone.utc
    )


    # ------------------------------------------------------------------------
    # Step 3: Load the real Bronze table
    # ------------------------------------------------------------------------
    #
    # The DQ checks must run against the actual data produced by ingestion.
    #
    # This ensures that the validation layer is checking the same Bronze
    # table that downstream Silver processing will consume.
    # ------------------------------------------------------------------------

    bronze_df = spark.table(
        BRONZE_TABLE
    )


    # =========================================================================
    # Step 4: Run Data Quality Checks
    # =========================================================================

    # ------------------------------------------------------------------------
    # Coverage Check
    # ------------------------------------------------------------------------
    #
    # Checks whether every expected article/date combination exists.
    #
    # Example:
    #
    #     10 articles × 31 days = 310 expected combinations
    #
    # If only 287 exist, the check identifies the 23 missing combinations.
    # ------------------------------------------------------------------------

    coverage_result = check_coverage(
        df=bronze_df,
        spark=spark,
        article_titles=ARTICLE_TITLES,
        start_date=START_DATE,
        end_date=END_DATE,
    )


    # ------------------------------------------------------------------------
    # Duplicate Check
    # ------------------------------------------------------------------------
    #
    # Checks whether the article/timestamp business key appears more than
    # once in Bronze.
    # ------------------------------------------------------------------------

    duplicate_result = check_duplicates(
        df=bronze_df
    )


    # ------------------------------------------------------------------------
    # Schema Check
    # ------------------------------------------------------------------------
    #
    # Checks whether the Bronze table has:
    #
    #     - All expected columns
    #     - Correct data types
    #     - No unexpected columns
    # ------------------------------------------------------------------------

    schema_result = check_schema(
        df=bronze_df,
        expected_schema=EXPECTED_SCHEMA,
    )


    # =========================================================================
    # Step 5: Display Check Results
    # =========================================================================
    #
    # These messages make the Databricks Job output easier to understand when
    # investigating a particular execution.
    # =========================================================================

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

    print(
        f"{schema_result['check_name']}: "
        f"{schema_result['status']} | "
        f"mismatches={len(schema_result['mismatches'])}"
    )


    # ------------------------------------------------------------------------
    # Display schema mismatches when they exist
    # ------------------------------------------------------------------------
    #
    # If the schema check finds a problem, print the details so the Job
    # output immediately tells us what changed.
    # ------------------------------------------------------------------------

    if schema_result["mismatches"]:

        print("Schema mismatches:")

        for mismatch in schema_result["mismatches"]:
            print(
                f"  - {mismatch}"
            )


    # =========================================================================
    # Step 6: Normalize DQ Results
    # =========================================================================
    #
    # Each validation function can return information specific to its own
    # check.
    #
    # Before storing the results, we convert them into one consistent schema.
    #
    # This gives the DQ audit table a predictable structure:
    #
    #     check_run_id
    #     layer
    #     table_name
    #     check_name
    #     check_status
    #     expected_value
    #     actual_value
    #     details
    #     checked_at
    #
    # The validation functions do not need to know anything about this
    # audit-table structure.
    # ------------------------------------------------------------------------

    dq_rows = [
        {
            "check_run_id": check_run_id,
            "layer": "bronze",
            "table_name": BRONZE_TABLE,
            "check_name": coverage_result["check_name"],
            "check_status": coverage_result["status"],
            "expected_value": (
                len(ARTICLE_TITLES)
                * (
                    (
                        datetime.strptime(
                            END_DATE,
                            "%Y%m%d",
                        )
                        - datetime.strptime(
                            START_DATE,
                            "%Y%m%d",
                        )
                    ).days
                    + 1
                )
            ),
            "actual_value": str(
                coverage_result["missing_count"]
            ),
            "details": (
                f"Missing article/date combinations: "
                f"{coverage_result['missing_count']}"
            ),
            "checked_at": checked_at,
        },

        {
            "check_run_id": check_run_id,
            "layer": "bronze",
            "table_name": BRONZE_TABLE,
            "check_name": duplicate_result["check_name"],
            "check_status": duplicate_result["status"],
            "expected_value": "0",
            "actual_value": str(
                duplicate_result["duplicate_count"]
            ),
            "details": (
                f"Duplicate article/timestamp combinations: "
                f"{duplicate_result['duplicate_count']}"
            ),
            "checked_at": checked_at,
        },

        {
            "check_run_id": check_run_id,
            "layer": "bronze",
            "table_name": BRONZE_TABLE,
            "check_name": schema_result["check_name"],
            "check_status": schema_result["status"],
            "expected_value": "Expected schema",
            "actual_value": (
                "MATCH"
                if not schema_result["mismatches"]
                else "MISMATCH"
            ),
            "details": (
                "; ".join(
                    schema_result["mismatches"]
                )
                if schema_result["mismatches"]
                else "Schema matches expected schema."
            ),
            "checked_at": checked_at,
        },
    ]


    # =========================================================================
    # Step 7: Write DQ Results
    # =========================================================================
    #
    # The results are written BEFORE checking for failures.
    #
    # This is intentional.
    #
    # If the Job fails because of a DQ problem, we still want the failed
    # result recorded in the audit table so that we can investigate what
    # happened.
    # ------------------------------------------------------------------------

    dq_results_df = spark.createDataFrame(
        dq_rows
    )

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


    # =========================================================================
    # Step 8: Determine Whether the Pipeline Should Stop
    # =========================================================================
    #
    # WARN:
    #     The check found a problem, but the pipeline is allowed to continue.
    #
    # FAIL:
    #     The data-quality requirement was violated.
    #     Raise an exception so the Databricks Job fails.
    #
    # IMPORTANT:
    #
    # Our current validation functions return PASS or WARN.
    #
    # That means this block will only stop the pipeline once a check is
    # explicitly configured to return FAIL.
    #
    # We can later introduce a DQ policy that determines which checks should
    # be blocking.
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


    # =========================================================================
    # Step 9: Successful Completion
    # =========================================================================
    #
    # Reaching this point means there were no hard FAIL results.
    #
    # WARN results may still exist, but they have already been recorded in
    # the DQ audit table and do not block downstream processing.
    # ------------------------------------------------------------------------

    print(
        "Bronze DQ checks completed successfully. "
        f"check_run_id={check_run_id}"
    )

    return check_run_id


# ============================================================================
# Script Entry Point
# ============================================================================

if __name__ == "__main__":

    # Databricks provides the Spark session when this script is executed
    # through the appropriate Job/notebook environment.
    spark = SparkSession.builder.getOrCreate()

    run_id = run_bronze_dq_checks(
        spark
    )

    print(
        f"Bronze DQ run completed: {run_id}"
    )
