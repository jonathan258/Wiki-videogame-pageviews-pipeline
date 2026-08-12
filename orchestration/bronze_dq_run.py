"""
Runs the Bronze data quality checks against the real Bronze table
in Unity Catalog. This file is meant to be run inside Databricks,
since it needs a live Spark session connected to Unity Catalog —
it won't work running locally on your laptop.
"""

from validation import check_coverage, check_duplicates

# These need to match the same values used in pipeline_run.py.
# (Worth deciding later where these should really live so you're
# not maintaining the same list in two places.)
article_titles = [
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
start_date = "20260101"
end_date = "20260131"

catalog = "wiki_videogame_ingestion"
schema = "bronze"
table = "wiki_videogame_pageviews"

# Load the real Bronze table as a DataFrame so the checks can run
# against it.
bronze_df = spark.table(f"{catalog}.{schema}.{table}")

# Run the coverage check — did we get data for every article,
# every expected day?
coverage_result = check_coverage(
    df=bronze_df,
    spark=spark,
    article_titles=article_titles,
    start_date=start_date,
    end_date=end_date,
)

for field in bronze_df.schema:
    print(field.name, field.dataType.simpleString())
    
print(
    coverage_result["check_name"],
    coverage_result["status"],
    coverage_result["missing_count"],
)

# Run the duplicate check — does any article/day combination
# appear more than once?
duplicate_result = check_duplicates(df=bronze_df)
print(
    duplicate_result["check_name"],
    duplicate_result["status"],
    duplicate_result["duplicate_count"],
)
