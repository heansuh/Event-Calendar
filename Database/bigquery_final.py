# BigQuery final code for orchestration written by: Heansuh Lee

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
from datetime import datetime, timedelta, date
import re

def uploading_table_to_big_query(df,full_table_id,project_id,credentials):    # Uploading data to BigQuery.
    try:
        to_gbq(df, destination_table=full_table_id, project_id=project_id, if_exists='append', credentials=credentials, progress_bar=True)
        print("Data uploaded to BigQuery successfully!")   #  append doesnt change table structure.
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    # from sentence_transformers import SentenceTransformer, util
    # from rapidfuzz.fuzz import ratio

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

    project_id = credentials.project_id    # or just write:  'circular-maxim-436817-e3'
    dataset_id = 'events_data'
    # table_id_new = 'new_events'
    # full_table_id = f"{project_id}.{dataset_id}.{table_id_new}"
    # table_id_old = 'old_events'  # a table to store old_events (existing events)


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

    dataset_ref = f"{project_id}.{dataset_id}"

    # List tables in the dataset
    tables = client.list_tables(dataset_ref)

    # Print the table names
    print(f"Tables in dataset {dataset_id}:")
    for table in tables:
        print(table.table_id)

    today = date.today()

    format_today = today.strftime("%Y%m%d")

    table_name = "events_data_new_events_temp"

    # Full table reference
    table_ref = f"{dataset_ref}.{table_name}"

    # Load data into a DataFrame
    query = f"SELECT * FROM `{table_ref}`"
    combined_df = client.query(query).to_dataframe()
    
    ### Deduplication

    deduplicated_df = combined_df.copy()

    deduplicated_df['Counter'] = deduplicated_df.groupby(['Start Date', 'Start Time', 'Location', 'City'])['Start Date'].transform('size') - 1

    deduplicated_df['Counter'] = deduplicated_df['Counter'].fillna(0).astype(int)

    # Step 1: Group by the desired columns
    grouped = deduplicated_df.groupby(['Start Date', 'Start Time', 'Location', 'City'])

    # Step 2: Function to update the description for the first row in each group
    def process_group(group):
        # If there is more than one event in the group
        if len(group) > 1:
            # Prepare the summary of dropped events
            counter = len(group) - 1
            dropped_events = "\n".join(
                f"Event {row['Subject']}: {row['Description']}" 
                for _, row in group.iloc[1:].iterrows()
            )
            # Update the description of the first event
            group.iloc[0, group.columns.get_loc('Description')] = (
                f"{counter} event(s) found at the same time/location. Please check the following:\n"
                f"{dropped_events}\n"
                f"{group.iloc[0]['Description']}"
            )
        return group.iloc[0]  # Keep only the first row in the group

    # Step 3: Apply the function to each group and combine the results
    result_df = grouped.apply(process_group).reset_index(drop=True)

    df_cleaned = result_df[result_df['Start Date'].astype(str).notna() & (result_df['Start Date'].astype(str).str.strip() != '')]

    # Filter rows where Counter > 0 or Music_label is True
    filtered_df = df_cleaned[(df_cleaned['Counter'] > 0) | (df_cleaned['Music_label'] == True)]
    filtered_df = df_cleaned.reset_index(drop=True)

    # Get today's date
    today = pd.Timestamp(datetime.now().date())

    # Calculate the date 30 days from today
    thirty_days_from_today = today + timedelta(days=30)

    # Convert 'Start Date' back to datetime for filtering
    filtered_df['Start Date'] = pd.to_datetime(filtered_df['Start Date'], format='%Y-%m-%d')

    # Filter rows where Start Date is >= today, and either Counter > 0 or filtered_label is True
    filtered_df = filtered_df[
        (filtered_df['Start Date'] >= today) &
        (filtered_df['Start Date'] <= thirty_days_from_today) &  
        ((filtered_df['Counter'] > 0) | (filtered_df['Music_label'] == True))
    ].reset_index(drop=True)

    filtered_df['Colour'] = 1
    filtered_df.loc[filtered_df['Counter'] > 0, 'Colour'] = 4

    filtered_df.shape # Shape of filtered_df

    filtered_df.Counter.value_counts() # Distribution of Counter values

    table_id_new = f'{dataset_id}_new_events_{format_today}'

    full_table_id = f"{dataset_ref}.{table_id_new}"

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
    `Music_label` BOOL,
    `Counter` INT64,
    `Colour` INT64
    )"""

    query_job = client.query(create_table_query).result()
    print("Table created successfully.")

    uploading_table_to_big_query(filtered_df,full_table_id,project_id,credentials)  # this function uploads data to Big Query.

    # List tables in the dataset
    tables = client.list_tables(dataset_ref)

    # Retrieve table names
    table_names = [table.table_id for table in tables]

    # Regular expressions to match table types
    new_events_pattern = r"events_data_new_events_(\d{8})"
    old_events_pattern = r"events_data_old_events_(\d{8})"

    # Extract and store dates for each table type
    new_events = []
    old_events = []

    for table_name in table_names:
        new_match = re.search(new_events_pattern, table_name)
        old_match = re.search(old_events_pattern, table_name)

        if new_match:
            new_events.append((table_name, int(new_match.group(1))))  # Store table name and date
        elif old_match:
            old_events.append((table_name, int(old_match.group(1))))

    # Sort the tables by date and get the latest ones
    latest_new_table = max(new_events, key=lambda x: x[1], default=None)
    latest_old_table = max(old_events, key=lambda x: x[1], default=None)

    # Print results
    if latest_new_table:
        print(f"Latest new_events table: {latest_new_table[0]} (Date: {latest_new_table[1]})")
    else:
        print("No new_events table found.")

    if latest_old_table:
        print(f"Latest old_events table: {latest_old_table[0]} (Date: {latest_old_table[1]})")
    else:
        print("No old_events table found.")

    # List tables in the dataset
    tables = client.list_tables(dataset_ref)
    table_names = [table.table_id for table in tables]

    # Regular expressions to identify table types
    new_events_pattern = r"events_data_new_events_(\d{8})"
    old_events_pattern = r"events_data_old_events_(\d{8})"

    # Extract and store tables with their dates
    new_events = []
    old_events = []

    for table_name in table_names:
        new_match = re.search(new_events_pattern, table_name)
        old_match = re.search(old_events_pattern, table_name)
        
        if new_match:
            new_events.append((table_name, int(new_match.group(1))))  # Table name and date
        elif old_match:
            old_events.append((table_name, int(old_match.group(1))))

    # Today's date
    today = int(datetime.now().strftime("%Y%m%d"))

    # Find the latest new_events table (likely today's date)
    latest_new_table = max(new_events, key=lambda x: x[1], default=None)

    # Find the latest old_events table BEFORE today
    latest_old_table = max(
        [table for table in old_events if table[1] < today],
        key=lambda x: x[1],
        default=None
    )

    # Load DataFrames from the tables
    if latest_old_table:
        query_old = f"SELECT * FROM `{dataset_id}.{latest_old_table[0]}`"
        df_old = client.query(query_old).to_dataframe()
        print(f"Loaded df_old from table: {latest_old_table[0]}")
    else:
        df_old = pd.DataFrame()  # Empty DataFrame if no table is found
        print("No valid old_events table found before today.")

    if latest_new_table:
        query_new = f"SELECT * FROM `{dataset_id}.{latest_new_table[0]}`"
        df_new = client.query(query_new).to_dataframe()
        print(f"Loaded df_new from table: {latest_new_table[0]}")
    else:
        df_new = pd.DataFrame()  # Empty DataFrame if no table is found
        print("No valid new_events table found for today.")

    def normalize_time(value):
        if pd.notna(value) and isinstance(value, str):
            return value.zfill(5).replace('0000', '00:00')  # Ensure format HH:mm
        return value

    # Apply normalization to time columns
    for df in [df_old, df_new]:
        df['Start Time'] = df['Start Time'].apply(normalize_time)
        df['End Time'] = df['End Time'].apply(normalize_time)

    # Process each city independently
    cities = set(df_old['City'].dropna()).union(set(df_new['City'].dropna()))

    # Create an empty DataFrame to store results
    final_df = pd.DataFrame()

    # Loop over each city
    for city in cities:
        df_old_city = df_old[df_old['City'] == city]
        df_new_city = df_new[df_new['City'] == city]
        
        # Process old events and compare with the new dataset
        for _, old_row in df_old_city.iterrows():
            # Find matching events by Subject in the same city
            matching_subject = df_new_city[df_new_city['Subject'] == old_row['Subject']]

            if matching_subject.empty:
                # Event not found in new dataset: Mark as deleted (Red)
                deleted_event = old_row.copy()
                deleted_event['Colour'] = 11  # Red
                deleted_event['Start Time'] = normalize_time(old_row['Start Time'])
                deleted_event['End Time'] = normalize_time(old_row['End Time'])
                df_new_city = pd.concat([df_new_city, pd.DataFrame([deleted_event])], ignore_index=True)
            else:
                for idx, new_row in matching_subject.iterrows():
                    # Preserve duplicate (4) if it exists in the old dataset
                    if old_row['Colour'] == 4:
                        df_new_city.loc[idx, 'Colour'] = 4
                        continue

                    # Check for differences in specified columns
                    columns_to_check = ['Start Date', 'Start Time', 'End Date', 'End Time', 'Location', 'Category']
                    differences = {}

                    for col in columns_to_check:
                        if old_row[col] != new_row[col] and pd.notna(old_row[col]) and pd.notna(new_row[col]):
                            differences[col] = {
                                "Old Value": old_row[col],
                                "New Value": new_row[col]
                            }

                    if differences:
                        # Event details changed: Mark as changed (Yellow)
                        df_new_city.loc[idx, 'Colour'] = 5  # Yellow
                        # Update description with original values
                        original_description = new_row.get('Description', '')
                        for key, value in differences.items():
                            original_description += f"\nOriginal {key}: {value['Old Value']}"
                        df_new_city.loc[idx, 'Description'] = original_description
                    else:
                        # No changes: Set to Green (10) only if it isn't a duplicate (2)
                        if new_row.get('Colour', None) not in [4, 5, 11]:
                            df_new_city.loc[idx, 'Colour'] = 10  # Green for unchanged

        # Ensure remaining new events are marked appropriately
        for idx, new_row in df_new_city.iterrows():
            # Preserve duplicate (2) if already set
            if new_row.get('Colour', None) == 4:
                continue
            # Mark as green (10) for events without explicit updates
            if pd.isna(new_row.get('Colour', None)) or new_row['Colour'] not in [5, 11]:
                df_new_city.loc[idx, 'Colour'] = 10  # Green for unchanged

        # Append city's results to the final DataFrame
        final_df = pd.concat([final_df, df_new_city], ignore_index=True)
        final_df_kiel = final_df[final_df['City'] == 'Kiel']

    calendar_id = 'eventcalendarkiel@gmail.com'

    # Retrieve all events
    try:
        events_result = calendar_service.events().list(
            calendarId=calendar_id,
            maxResults=2500,  # Adjust if you have more events
            singleEvents=True
        ).execute()
        events = events_result.get('items', [])

        if not events:
            print("No events found.")
        else:
            # Loop through events and delete them
            for event in events:
                try:
                    event_id = event['id']
                    calendar_service.events().delete(calendarId=calendar_id, eventId=event_id).execute()
                    print(f"Deleted event: {event.get('summary', 'Unnamed Event')}")
                except Exception as e:
                    print(f"Error deleting event: {e}")

    except Exception as e:
        print(f"Error retrieving events: {e}")

    success_count = 0
    failure_count = 0

    # Loop through the DataFrame rows
    for i, row in final_df_kiel.iterrows():
        # Extract row data
        start_date_str = row['Start Date']
        end_date_str = row['End Date']
        start_time_str = row['Start Time']
        end_time_str = row['End Time']
        color_id = str(row['Colour'])  # Use the Colour column for the colorId

        try:
            # Generate timestamp for event creation
            creation_timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

            # Append timestamp to the description
            description_with_timestamp = f"{row['Description']}\n\nEvent created on: {creation_timestamp}"

            # Check if start_time is NaN or blank (indicating an all-day event)
            if pd.isna(start_time_str) or start_time_str.strip() == "":
                # Event is all day, use only date
                start_date_obj = pd.to_datetime(start_date_str).date()  # Convert to date
                end_date_obj = pd.to_datetime(end_date_str).date() if not pd.isna(end_date_str) else start_date_obj

                # Increment end date by 1 day if start and end dates are the same
                if start_date_obj == end_date_obj:
                    end_date_obj += pd.Timedelta(days=1)

                event = {
                    'summary': row['Subject'],  # Use the Subject column for the event name
                    'location': row['Location'],
                    'start': {
                        'date': start_date_obj.isoformat(),  # Use date for all-day event
                    },
                    'end': {
                        'date': end_date_obj.isoformat(),  # End date for all-day event
                    },
                    'description': description_with_timestamp,  # Updated description
                    'colorId': color_id  # Set the color based on the Colour column
                }
            else:
                # Combine date and time for start
                start_datetime_str = f"{start_date_str} {start_time_str}"  # Replace "T" with a space
                start_time_obj = pd.to_datetime(start_datetime_str)

                # Combine date and time for end
                if pd.isna(end_time_str) or end_time_str.strip() == "":
                    end_time_obj = start_time_obj + pd.Timedelta(hours=1)  # Default duration: 1 hour
                else:
                    end_datetime_str = f"{end_date_str} {end_time_str}"  # Replace "T" with a space
                    end_time_obj = pd.to_datetime(end_datetime_str)

                # Increment end date by 1 day if start and end times overlap midnight
                if start_time_obj.date() == end_time_obj.date() and end_time_obj.time() == pd.to_datetime("00:00").time():
                    end_time_obj += pd.Timedelta(days=1)

                event = {
                    'summary': row['Subject'],  # Use the Subject column for the event name
                    'location': row['Location'],
                    'start': {
                        'dateTime': start_time_obj.strftime("%Y-%m-%dT%H:%M:%S"),
                        'timeZone': 'Europe/Berlin',  # Set to your timezone
                    },
                    'end': {
                        'dateTime': end_time_obj.strftime("%Y-%m-%dT%H:%M:%S"),
                        'timeZone': 'Europe/Berlin',  # Set to your timezone
                    },
                    'description': description_with_timestamp,  # Updated description
                    'colorId': color_id  # Set the color based on the Colour column
                }

            # Create the event in Google Calendar
            created_event = calendar_service.events().insert(calendarId=calendar_id, body=event).execute()
            print(f"Event created: {created_event.get('htmlLink')}")
            success_count += 1
        except Exception as e:
            print(f"Error creating event for row {i}: {e}")
            failure_count += 1

    # Print the summary of operations
    print(f"Success count: {success_count}")
    print(f"Failure count: {failure_count}")

main()
