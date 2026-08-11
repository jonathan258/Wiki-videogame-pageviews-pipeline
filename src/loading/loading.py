'''What you're building: a function that takes your
flat list of dicts, turns it into a Spark DataFrame,
tags it with audit columns, and writes it as a Delta table in Unity Catalog.

Step 1 — Look up: spark.createDataFrame()
to createDataFrame from list of dicts
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.appName("PageviewsLoader").getOrCreate()

# Spark is the big engine that reads, transforms, and writes data.
# We create one session here so we can build DataFrames and write Delta tables.


def write_bronze(
    records: list[dict],
    catalog: str,
    schema: str,
    table: str,
    ingestion_job_run_id: str,
) -> None:
    # Turn the list of Python dicts into a Spark DataFrame.
    df = spark.createDataFrame(records)

    # Add two audit columns so we know when and why the data was loaded.
    df_with_audit = (
        df.withColumn("_ingested_at", current_timestamp())
          .withColumn("_ingestion_job_run_id", lit(ingestion_job_run_id))
    )

    # Show the data and schema in the console for quick checking.
    df_with_audit.show()
    df_with_audit.printSchema()

    # Build the full Unity Catalog table name and write the data as Delta.
    target_table = f"{catalog}.{schema}.{table}"
    df_with_audit.write.format("delta").mode("append")\
        .saveAsTable(target_table)

