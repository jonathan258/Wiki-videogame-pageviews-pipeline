import sys
import os

sys.path.append(os.path.abspath("../src"))
from ingestion.ingestion import fetch_api_data
from transform.flatten import flatten_results
from loading.loading import write_bronze

# Set up the fixed values for this notebook run.
# Replace the catalog and schema names with values that exist
# in Databricks workspace.
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
ingestion_job_run_id = "manual_test_run_001"

# Fetch the API data for each article title.
results = {}
for title in article_titles:
    data = fetch_api_data(title, start_date, end_date)
    if data is not None:
        results[title] = data

# Flatten the nested results into a single list of records.
all_records = []
for title, response_data in results.items():
    all_records.extend(flatten_results({title: response_data}))

# Write the flattened data into the bronze Delta table.
write_bronze(
    records=all_records,
    catalog=catalog,
    schema=schema,
    table=table,
    ingestion_job_run_id=ingestion_job_run_id,
)
