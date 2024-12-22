import os
import logging
from google.oauth2 import service_account
from google.cloud import bigquery
from pandas_gbq import to_gbq
import pandas as pd
from datetime import datetime
from pathlib import Path

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bigquery_upload_log.log"),
        logging.StreamHandler(),
    ],
)

# Environment variables (best practice for Cloud Run)
CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_credentials.json")
PROJECT_ID = os.getenv("BIGQUERY_PROJECT", "lucky-reactor-443308-r4")
DATASET_ID = os.getenv("BIGQUERY_DATASET", "events_data")
TABLE_NAME_PREFIX = "events_data_new_events_temp"

# Paths
MERGED_FILE_PATH = Path("./finalized_scrapers/merged_data.csv")

# Debug: Log current working directory and contents
logging.debug(f"Current working directory: {os.getcwd()}")
logging.debug(f"Files in directory: {list(Path('.').iterdir())}")

# Load credentials and BigQuery client
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def create_table_if_not_exists(table_id):
    """
    Creates a BigQuery table with a defined schema if it doesn't exist.

    Args:
        table_id (str): Fully qualified table ID in BigQuery.
    """
    schema = [
        bigquery.SchemaField("Subject", "STRING"),
        bigquery.SchemaField("Start_Date", "DATE"),
        bigquery.SchemaField("End_Date", "DATE"),
        bigquery.SchemaField("Start_Time", "STRING"),
        bigquery.SchemaField("End_Time", "STRING"),
        bigquery.SchemaField("Location", "STRING"),
        bigquery.SchemaField("City", "STRING"),
        bigquery.SchemaField("Description", "STRING"),
        bigquery.SchemaField("Category", "STRING"),
        bigquery.SchemaField("Music_label", "BOOL"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    try:
        client.create_table(table)
        logging.info(f"Table created: {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            logging.info(f"Table already exists: {table_id}")
        else:
            logging.error(f"Error creating table: {e}")
            raise e

def upload_to_bigquery():
    """
    Uploads the merged CSV data to BigQuery.
    """
    if not MERGED_FILE_PATH.exists():
        logging.error(f"Merged data file not found at: {MERGED_FILE_PATH}. Exiting upload process.")
        return

    # Generate dynamic table name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_PREFIX}_{timestamp}"

    # Ensure table exists
    create_table_if_not_exists(table_id)

    # Load and clean merged data
    try:
        df = pd.read_csv(MERGED_FILE_PATH, index_col=0)

        # Rename columns to match BigQuery schema
        column_mapping = {
            "Start_date": "Start_Date",
            "End_date": "End_Date",
            "Start_time": "Start_Time",
            "End_time": "End_Time",
        }
        df.rename(columns=column_mapping, inplace=True)

        # Log data insights
        logging.info(f"Columns after renaming: {df.columns.tolist()}")
        logging.debug(f"First 5 rows:\n{df.head().to_string(index=False)}")

        # Convert date columns and drop rows with missing required dates
        df["Start_Date"] = pd.to_datetime(df["Start_Date"], errors="coerce").dt.date
        df["End_Date"] = pd.to_datetime(df["End_Date"], errors="coerce").dt.date
        df.dropna(subset=["Start_Date", "End_Date"], inplace=True)
        logging.info(f"Data cleaned and ready for upload. {len(df)} rows remaining.")
    except Exception as e:
        logging.error(f"Error loading and cleaning data: {e}")
        return

    # Upload to BigQuery
    try:
        to_gbq(
            dataframe=df,
            destination_table=table_id,
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists="replace",
            progress_bar=True,
        )
        logging.info(f"Data successfully uploaded to BigQuery table: {table_id}")
    except Exception as e:
        logging.error(f"Failed to upload data to BigQuery: {e}")

if __name__ == "__main__":
    logging.info("Starting upload to BigQuery...")
    upload_to_bigquery()
    logging.info("BigQuery upload process completed.")
