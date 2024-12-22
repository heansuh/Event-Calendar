# BigQuery final code for orchestration written by: Heansuh Lee

### Libraries

# using service account only to connect to BigQuery and Calendar.
from googleapiclient.discovery import build
from datetime import timedelta
from googleapiclient.http import BatchHttpRequest
from google.oauth2 import service_account
from google.cloud import bigquery
from pandas_gbq import to_gbq
from googleapiclient.errors import HttpError
import time
import datetime as datetime
import pandas as pd  
import numpy as np
from datetime import datetime, timedelta

# from sentence_transformers import SentenceTransformer, util
# from rapidfuzz.fuzz import ratio

### Credentials

SERVICE_CREDENTIALS = 'service_credentials_final.json'

# Define both scopes for Calendar and BigQuery
SCOPES = [
    "https://www.googleapis.com/auth/calendar",  # Google Calendar
    "https://www.googleapis.com/auth/bigquery"   # BigQuery
]

# Create credentials with both scopes
credentials = service_account.Credentials.from_service_account_file(SERVICE_CREDENTIALS, scopes=SCOPES)

# Create BigQuery client and Calendar service using the same credentials.
client = bigquery.Client(credentials=credentials, project=credentials.project_id)
calendar_service = build('calendar', 'v3', credentials=credentials)

### Testing BigQuery Connection

project_id = credentials.project_id
dataset_id = 'events_data'
# table_id_new = 'new_events'
# full_table_id = f"{project_id}.{dataset_id}.{table_id_new}"
# table_id_old = 'old_events'  # a table to store old_events (existing events).

def test_bigquery_connection(client):
    query = f"""
    SELECT COUNT(*)
    FROM `{project_id}.{dataset_id}.__TABLES_SUMMARY__`
    """
    try:
        query_job = client.query(query)
        result = next(query_job.result())  # Fetch result to ensure query runs
        print("BigQuery connection successful. Number of tables:", result[0])
    except Exception as e:
        print("Error connecting to BigQuery:", e)

# Test the BigQuery connection
test_bigquery_connection(client)

### Combining all .csv files into one DataFrame, and saving it as 1 .csv file

import os
import pandas as pd

# Specify the folder containing the original CSV files
input_folder = 'csv'
output_folder = 'processed_csv'

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

# Columns to extract and their renamed counterparts
columns_mapping = {
    'Subject': 'Subject',
    'Start_date': 'Start Date',
    'End_date': 'End Date',
    'Start_time': 'Start Time',
    'End_time': 'End Time',
    'Location': 'Location',
    'City': 'City',
    'Description': 'Description',
    'Category': 'Category',
    'Music_label': 'Music_label'
}

# List to store processed DataFrames
processed_dataframes = []
# List to store modified City column values
city_values = []

# Function to convert German umlauts and spaces
def format_city_name(city_name):
    city_name = city_name.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')
    city_name = city_name.replace('Ä', 'Ae').replace('Ö', 'Oe').replace('Ü', 'Ue')
    city_name = city_name.replace('ß', 'ss')
    city_name = city_name.replace(' ', '_')
    city_name = city_name.replace('/', '')  # Remove forward slashes
    city_name = city_name.replace('.', '').replace(',', '').replace('(', '').replace(')', '').replace('-', '')
    # List of target cities to normalize
    target_cities = ["Hamburg", "Luebeck", "Norderstedt", "Hannover", "Bremen", "Braunschweig"]
    
    # Check if city_name contains any target city
    for target_city in target_cities:
        if target_city in city_name:
            city_name = target_city
            break
    if city_name == 'inselweit':
        city_name = 'Amrum_inselweit'
    return city_name

# Process each CSV file in the input folder
for file in os.listdir(input_folder):
    if file.endswith('.csv'):
        input_file_path = os.path.join(input_folder, file)
        
        # Read the CSV file
        df = pd.read_csv(input_file_path, index_col=0)
        
        # Extract and rename the required columns
        extracted_df = df[list(columns_mapping.keys())].rename(columns=columns_mapping)
        
        # Format the City column and store unique values
        extracted_df['City'] = extracted_df['City'].apply(format_city_name)
        city_values.extend(extracted_df['City'].unique())
        
        # Save the processed DataFrame to a new CSV file
        output_file_name = file.replace('.csv', '_direct.csv')
        output_file_path = os.path.join(output_folder, output_file_name)
        extracted_df.to_csv(output_file_path, index=False)
        
        # Append the processed DataFrame to the list
        processed_dataframes.append(extracted_df)

# Combine all processed DataFrames into one
combined_df = pd.concat(processed_dataframes, ignore_index=True)
# Convert uppercase city names to title case
combined_df['City'] = combined_df['City'].apply(lambda x: x.title() if x.isupper() else x)
# Remove rows with empty City values
combined_df = combined_df[combined_df['City'] != '_']
# Drop rows where 'City' contains 'http'
combined_df = combined_df[~combined_df['City'].str.contains('http', case=False, na=False)]
# Drop rows where 'City' is empty or null
combined_df = combined_df[combined_df['City'].notna() & (combined_df['City'].str.strip() != '')]

# Save the combined DataFrame to a single CSV file
combined_file_path = os.path.join(output_folder, 'combined_data.csv')
combined_df.to_csv(combined_file_path, index=False)

# # Save the unique City names to a file
# unique_city_file_path = os.path.join(output_folder, 'unique_cities.txt')
# with open(unique_city_file_path, 'w') as f:
#     for city in sorted(set(city_values)):
#         f.write(city + '\n')

print(f"Processed files saved to {output_folder}")
print(f"Combined DataFrame saved as {combined_file_path}")
# print(f"Unique City names saved as {unique_city_file_path}")

### Creating table in the dataset

dataset_id = 'events_data'
table_id_new = f'{dataset_id}_new_events_temp'

full_table_id = f"{project_id}.{dataset_id}.{table_id_new}"

create_table_query = f"""
CREATE OR REPLACE TABLE `{full_table_id}` (
  `Subject` STRING,
  `Start Date` DATE,  -- Defines Start_date as DATE type.
  `End Date` DATE,    -- Defines Endt_date as DATE type.
  `Start Time` STRING,
  `End Time` STRING,
  `Location` STRING,
  `Description` STRING,
  `Category` STRING,
  `City` STRING,
  `Music_label` BOOL
)"""

query_job = client.query(create_table_query).result()
print("Table created successfully.")

### Convert Start Date and End Date to datetime

combined_df['Start Date'] = pd.to_datetime(combined_df['Start Date'], errors='coerce', format='%Y-%m-%d')
combined_df = combined_df.dropna(subset=['Start Date'])
combined_df['End Date'] = pd.to_datetime(combined_df['End Date'], errors='coerce', format='%Y-%m-%d')
combined_df = combined_df.dropna(subset=['End Date'])

### Uploading data to BigQuery

def uploading_table_to_big_query(df):    # Uploading data to BigQuery.
    try:
        to_gbq(df, destination_table=full_table_id, project_id=project_id, if_exists='replace', credentials=credentials, progress_bar=True)
        print("Data uploaded to BigQuery successfully!")   #  append doesnt change table structure.
    except Exception as e:
        print(f"An error occurred: {e}")

### Upload temporary processed data to BigQuery

uploading_table_to_big_query(combined_df)  # this function uploads data to Big Query.