# The code is written by Ilia Semenok.

# connecting to big query and calendar.
# deleting events from calendar.
    # starting the first webscraper.
    # sending the data that was scraped from the first scraper to Big Query Database.
    # starting the second webscraper.
    # sending the data that was scraped from the second scraper to Big Query Database.
    # and so one until all scrapers are done.
# loading data back to python to do duplication removal and other preprocessing stuff.
# uploading this data back to BIg Query for event comparison with past events.
# check if "old_events" table exist in the database, if not send data to it from "new_events" table.
# doing event comparison and adding one extra summary column.
# turning data in the final format for calendar and Finally inserting events into Calendar.
# Replace 'old_events' table data with the data from the 'new_events' table.


# 1. Connecting to Big Query and Calendar

# using service account only to connect to BigQuery and Calendar.
from googleapiclient.discovery import build
from datetime import timedelta, datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from pandas_gbq import to_gbq
from googleapiclient.errors import HttpError
import time
import pandas as pd
import numpy as np

SERVICE_CREDENTIALS = 'service_credentials.json'

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

# 2. Checking Big Query Connection

project_id = credentials.project_id   
dataset_id = 'your_dataset_id'  # replace it with your dataset_id
table_id_new = 'new_events'
full_table_id = f"{project_id}.{dataset_id}.{table_id_new}"
table_id_old = 'old_events'  # a table to store old_events (existing events).

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

# 3. Deleting events from Calendar

def fetch_all_events(service):
    """This returns the total amount of events currently being in calendar. (all event IDs from the calendar). """
    events = []
    page_token = None
    while True:
        events_result = service.events().list(calendarId='your_email@gmail.com', maxResults=1000,
                                              singleEvents=True,  
                                              pageToken=page_token).execute()

        events.extend(events_result.get('items', []))
        page_token = events_result.get('nextPageToken')
        if not page_token:
            break       
    return [event['id'] for event in events]

event_list = fetch_all_events(calendar_service)
len(event_list) 

def delete_events(event_list, calendar_id='your_email@gmail.com', max_retries=3):
    import time
    from googleapiclient.errors import HttpError

    delete_event_count = 0
    retry_count = 0

    while event_list and retry_count < max_retries:
        failed_events = []
        for event_id in event_list:
            try:
                calendar_service.events().delete(calendarId=calendar_id, eventId=event_id).execute()
                # print(f'Successfully deleted event {event_id}')
                delete_event_count += 1
                if delete_event_count % 580 == 0:
                    print("450 events deleted, pausing for 60 seconds...")
                    time.sleep(60)
            except HttpError as e:
                if e.resp.status in [503, 403]:
                    print(f"Error deleting event: {event_id} with error: {e}")
                    failed_events.append(event_id)
            except Exception as e:
                print(f"Unknown Error deleting event {event_id}: {e}")

        print(f"Successfully deleted {delete_event_count} events from calendar")
        print(f"Number of failed events: {len(failed_events)}")
        
        event_list = failed_events  # the loop continues if failed_events took place. If not then the loop stops.
        retry_count += 1
        # Sleep between retries
        if event_list:
            print("Pausing for 60 seconds before retrying...")
            time.sleep(60)
        print(f"Retry {retry_count}: {len(event_list)} failed events remaining.")

    print(f"Finished deletion process. Successfully deleted {delete_event_count} events.")
    if event_list:
        print(f"Failed to delete {len(event_list)} events after {max_retries} retries: {event_list}")

delete_events(event_list)

# 4. Create table in database to insert our data to.

create_table_query = f"""
CREATE OR REPLACE TABLE `{full_table_id}` (
  `Subject` STRING,
  `Start_date` DATE,  -- Defines Start_date as DATE type.
  `End_date` DATE,    -- Defines End_date as DATE type.
  `Start_time` STRING,
  `End_time` STRING,
  `Location` STRING,
  `Description` STRING,
  `City` STRING,
  `Category` STRING,
  `Music_label` BOOLEAN
)"""
query_job = client.query(create_table_query).result()
print("Table created successfully.")

# 5. Upload data to the database (to "new_events" table).
# here you must run your webscraper and the function below will send this data to the database.

def uploading_table_to_big_query(df):    # Uploading data to BigQuery.
    try:
        to_gbq(df, destination_table=full_table_id, project_id=project_id, if_exists='append', credentials=credentials, progress_bar=True)
        print("Data uploaded to BigQuery successfully!")   #  append doesnt change table structure.
    except Exception as e:
        print(f"An error occurred: {e}")

uploading_table_to_big_query(df)

# 6. Loading data back to python to do duplication removal and other stuff.

loading_query = f"""select * from `{full_table_id}`"""
df = client.query(loading_query).to_dataframe()  # execute the query and convert to DataFrame.
print(df.shape)

# 7. Duplication removal step. Also adding Load_date column and concatenating Description column.

def remove_duplicates(df):
    from rapidfuzz.fuzz import ratio  
    from datetime import datetime

    df = df.drop_duplicates(subset=['Subject', 'Start_date', 'Start_time', 'End_time', 'City'], keep='first').reset_index(drop=True)
    df = df.drop_duplicates(subset=['Start_date', 'Start_time', 'End_time', 'Location', 'City'], keep='first').reset_index(drop=True)

    df = df.sort_values(by=['Subject', 'Start_date', 'Start_time', 'City']).reset_index(drop=True)
    
    to_drop = set()  # Prepare for duplicate removal.

    # Group by 'Start_date', 'Start_time'
    for _, group in df.fillna({'Start_time': 'missing time', 'End_time': 'missing time'}).groupby(['Start_date', 'Start_time', 'City']):
        group = group.reset_index()

        # Use a set to keep track of indices already compared in the group
        compared_indices = set()

        # Compare rows within the group
        for i in range(len(group)):
            if group.loc[i, 'index'] in to_drop:
                continue
            for j in range(i + 1, len(group)):
                if group.loc[j, 'index'] in to_drop or j in compared_indices:
                    continue

                # Compute Levenshtein similarity
                lev_similarity = ratio(group.at[i, 'Subject'], group.at[j, 'Subject']) / 100.0

                # Check if similarity exceeds the threshold
                if lev_similarity > 0.9:
                    to_drop.add(group.loc[j, 'index'])  # Mark duplicate for removal
                    compared_indices.add(j)  # Avoid comparing this index again

    # Remove duplicates
    df = df.drop(index=to_drop).reset_index(drop=True)

    df['Load_date'] = datetime.today().strftime('%d-%m-%Y')  # below is concatenation of columns.
    df['Description'] = "Ladedatum: " + df['Load_date'] + ", " + df['Category'] + ", " + df['Description'] + ", " + df['City']
    # Add "Musik" to the 'Description' column where 'Music_label' is True
    df.loc[df['Music_label'] == True, 'Description'] += " Musik"
    print(df.shape)
    return df

df = remove_duplicates(df)

# 8. Uploading data back to Big Query for event comparison.

create_table_query = f"""
create or replace table `{full_table_id}` (
  `Subject` string,
  `Start_date` date,  -- Defines Start_date as DATE type.
  `End_date` date,    -- Defines Endt_date as DATE type.
  `Start_time` string,
  `End_time` string,
  `Location` string,
  `Description` string,
  `City` string,
  `Category` string,
  `Music_label` boolean,
  `Load_date` string
)"""
query_job = client.query(create_table_query).result()
print("Table created successfully.")

uploading_table_to_big_query(df)  # this function uploads data to Big Query.

# 9. Check if "old_events" table exist in the database, if not, create it and send data to it from "new_events" table.
# Only needed for the first time when running the code actually.

check_query = f"""
SELECT COUNT(*) as table_count
FROM `{project_id}.{dataset_id}.__TABLES_SUMMARY__`
WHERE table_id = '{table_id_old}'
"""

check_job = client.query(check_query).result()
table_exists = next(check_job)['table_count'] > 0

# If table doesn't exist, insert the new events in it from the 'new_events' table.
if not table_exists:
    print(f"The table '{table_id_old}' doesn't exist. Let's create one.")
    insert_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id_old}` AS
    SELECT * FROM `{project_id}.{dataset_id}.{table_id_new}`
    """
    client.query(insert_query)
    print('All done')

# 10. SQL query for event comparison.

query = f"""
with old_events as (
    select *,
    rank() over (partition by Subject order by Start_date, Start_time) as Event_rank
    from (
        select *,
        from {project_id}.{dataset_id}.{table_id_old}
        where End_date >= current_date()
        )
    
    ),
today_events as (
    select *,
    rank() over (partition by Subject order by Start_date, Start_time) as Event_rank
    from (
        select *,
        from {project_id}.{dataset_id}.{table_id_old}
        where End_date >= current_date()
        )
    )

    select 
    coalesce(t.Subject, p.Subject) AS Subject,       -- Capture events from either today or yesterday
    coalesce(t.Location, p.Location) AS Location,    -- Use today's location if available, otherwise yesterday's
    coalesce(t.`Start_date`, p.`Start_date`) AS Start_date,
    coalesce(t.`End_date`, p.`End_date`) AS End_date,
    coalesce(t.`Start_time`, p.`Start_time`) AS Start_time,
    coalesce(t.`End_time`, p.`End_time`) AS End_time,
    coalesce(t.Description, p.Description) AS Description,
    coalesce(t.City, p.City) AS City,  
    coalesce(t.Music_label, p.Music_label) AS Music_label, 

    -- Description based on changes
    CASE 
        WHEN t.Subject IS NULL AND p.Subject IS NOT NULL THEN CONCAT(p.Description, ' | event deleted')  
        WHEN t.Location != p.Location         THEN CONCAT(t.Description, ' | previous location: ', p.Location)
        WHEN t.`Start_date` != p.`Start_date` AND t.`Start_time` != p.`Start_time` THEN 
        CONCAT(t.Description, ' | previous date : ', FORMAT_DATE('%d.%m.%Y', p.`Start_date`), ' at: ', p.`Start_time`)
        WHEN t.`Start_date` != p.`Start_date` THEN 
        CONCAT(t.Description, ' | previous date: ', FORMAT_DATE('%d.%m.%Y', p.`Start_date`))
        WHEN t.`Start_time` != p.`Start_time` THEN CONCAT(t.Description, ' | previous time: ', p.`Start_time`)
        ELSE t.Description
    END AS Updated_Description,

    -- Status to track changes: changed, unchanged or deleted events.
    CASE 
        WHEN t.Subject IS NULL AND p.Subject IS NOT NULL THEN 'deleted'  -- Event present yesterday but missing today
        WHEN t.Location != p.Location OR t.`Start_date` != p.`Start_date` OR t.`Start_time` != p.`Start_time` THEN 'changed'
        ELSE 'unchanged'
    END AS status


    from today_events t
    full outer join old_events p                                    -- p stands for previous (old events).
    on LOWER(REGEXP_REPLACE(t.Subject, r'[^a-zA-Z0-9]', '')) = 
       LOWER(REGEXP_REPLACE(p.Subject, r'[^a-zA-Z0-9]', ''))
    and t.Event_rank = p.Event_rank and t.City = p.City
    -- where coalesce(t.`End_date`, p.`End_date`) >= current_date()  not needed.
"""
# execute the query.
query_job = client.query(query).result().to_dataframe()

# 11. Add one summary row in the final dataframe.

def add_summary_row(df):
    # Count the occurrences of each status value
    deleted_events = len(df[df["status"] == "deleted"])
    changed_events = len(df[df["status"] == "changed"])
    unchanged_and_new_events = len(df[df["status"] == "unchanged"])

    # Ensure the values default to 0 if there are no occurrences
    deleted_events = deleted_events if deleted_events else 0
    changed_events = changed_events if changed_events else 0
    unchanged_and_new_events = unchanged_and_new_events if unchanged_and_new_events else 0

    # Prepare the description
    description = f"Gesamtzahl der Veranstaltungen: {df.shape[0]}. \n" \
              f"Anzahl der gelöschten Veranstaltungen: {deleted_events}.\n" \
              f"Anzahl der geänderten Veranstaltungen: {changed_events}.\n" \
              f"Anzahl der unveränderten/neuen Veranstaltungen: {unchanged_and_new_events}."

    today_date = datetime.today().strftime('%Y-%m-%d')
    today_date_2 = datetime.today().strftime('%d.%m.%Y') # just a different format for the Subject column.

    summary_row = {
        'Subject': f'Zusammenfassung der heutigen Veranstaltungen vom {today_date_2}',
        'Location': 'You can only see this here. Neumünster, Wyk, Kiel, Heide, Husum, Hamburg, Lübeck',
        'Start_date': today_date,  
        'End_date': today_date,
        'Start_time': None,
        'End_time': None,
        'Description': description,
        'City': 'nowhere',
        'Music_label': False,
        'Updated_Description': description,
        'status': 'unchanged'
    }
    summary_df = pd.DataFrame([summary_row])             # convert the dictionary to a DataFrame with one row.
    df = pd.concat([df, summary_df], ignore_index=True)  # concatenate the summary row to the original dataframe.
    return df

df = add_summary_row(query_job)

# 12. Turn data into the final format for uploading events in Calendar.

def create_final_events_format(df):
    """Just preparing (turning them into the right format) events for the future insertion."""
    events_list = []
    for i, row in df.iterrows():

        start_date_str = row['Start_date']
        end_date_str = row['End_date']
        start_time_str = row['Start_time']
        end_time_str = row['End_time'] 

        # Determine the color based on the 'status' column.
        if row['status'] == 'changed':
            color_id = '5'   # Yellow color for changed events.
        elif row['status'] == 'deleted':
            color_id = '11'  # Red color for deleted events.
        elif row['Music_label'] == True:
            color_id = '9'
        else:
            color_id = '10'  # Green color for unchanged events.
            
        # Check if start_time is NaN
        if pd.isna(start_time_str):
            # Event is all day, use only date
            start_date_obj = pd.to_datetime(start_date_str).date()  # Convert to date
            end_date_obj = pd.to_datetime(end_date_str).date()      # Convert to date

            # Adjust for events that span midnight
            if end_time_obj <= start_time_obj:
                end_time_obj += timedelta(days=1)  # Move end time to the next day. [29.11.2024]

            event = {
                'summary': row['Subject'],
                'location': row['Location'],
                'start': {
                    'date': start_date_obj.isoformat(),  # Use date for all-day event
                },
                'end': {
                    'date': end_date_obj.isoformat(),  # End date for all-day event
                },
                'description': row['Updated_Description'],  # Include the prepared description
                'colorId': color_id  # Set the color based on the status
            }
        else:
            # Combine date and time for start
            start_datetime_str = f"{start_date_str}T{start_time_str}"
            start_time_obj = pd.to_datetime(start_datetime_str)

            # Combine date and time for end, or add a default duration if end time is None
            if pd.isna(end_time_str):  # If 'End_time' is None
                end_datetime_str = f"{end_date_str}T{start_time_str}"
                end_time_obj = pd.to_datetime(end_datetime_str) + timedelta(minutes=27)
            else:
                end_datetime_str = f"{end_date_str}T{end_time_str}"
                end_time_obj = pd.to_datetime(end_datetime_str)

            # Adjust for events that span midnight
            if end_time_obj <= start_time_obj:
                end_time_obj += timedelta(days=1)  # Move end time to the next day. [29.11.2024]
            
            # Prepare the event details for Google Calendar:
            event = {
                'summary': row['Subject'],
                'location': row['Location'],
                'start': {
                    'dateTime': start_time_obj.strftime("%Y-%m-%dT%H:%M:%S"),
                    'timeZone': 'Europe/Berlin',  # Set to your timezone
                },
                'end': {
                    'dateTime': end_time_obj.strftime("%Y-%m-%dT%H:%M:%S"),
                    'timeZone': 'Europe/Berlin',  # Set to your timezone
                },
                'description': row['Updated_Description'],  # Include the prepared description
                'colorId': color_id  # Set the color based on the status
            }
        events_list.append(event)
    return events_list
    
events_list_to_insert = create_final_events_format(df)

# 13. Insert events in Calendar.

def inserting_event_into_calendar(events_list_to_insert, calendar_id='your_email@gmail.com', max_retries=3):
    """Finally inserting events into Calendar"""
    import time
    from googleapiclient.errors import HttpError
    insert_event_count = 0
    retry_count = 0

    while events_list_to_insert and retry_count < max_retries:
        failed_events = []
        for event in events_list_to_insert:
            try:
                calendar_service.events().insert(calendarId=calendar_id, body=event).execute()
                # print(f'Successfully inserted event {event['summary']}')
                insert_event_count += 1
                if insert_event_count % 580 == 0:
                    print("450 events inserted, pausing for 60 seconds...")
                    time.sleep(60)
            except HttpError as e:
                if e.resp.status in [503, 403]:
                    print(f"Error inserting event: {event['summary']} with error: {e}")
                    failed_events.append(event)
            except Exception as e:
                print(f"Unknown Error inserting event {event['summary']}: {e}")

        print(f"Successfully inserted {insert_event_count} events into calendar")
        print(f"Number of failed events: {len(failed_events)}")
        
        events_list_to_insert = failed_events  # the loop continues if failed_events took place. If not then the loop stops.
        retry_count += 1
        # Sleep between retries
        if events_list_to_insert:
            print("Pausing for 60 seconds before retrying...")
            time.sleep(60)

        print(f"Retry {retry_count}: {len(events_list_to_insert)} failed events remaining.")

    print(f"Finished insertion process. Successfully inserted {insert_event_count} events.")
    if events_list_to_insert:
        print(f"Failed to insert {len(events_list_to_insert)} events after {max_retries} retries: {events_list_to_insert}")

inserting_event_into_calendar(events_list_to_insert)


# 14. Send new events in the "old_events" table.
replace_query = f"""
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id_old}` AS
SELECT * FROM `{project_id}.{dataset_id}.{table_id_new}`
"""
try:
    query_job = client.query(replace_query)  # Start the query job
    query_job.result()  # Wait for the job to complete
    print(f"The table '{table_id_old}' has been replaced with data from '{table_id_new}'.")
except Exception as e:
    print(f"An error occurred: {e}")


