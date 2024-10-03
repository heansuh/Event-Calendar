import os.path
import datetime as dt

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/calendar"]

def main():
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
    
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.json", "w") as token:
            token.write(creds.to_json())
    
    try:
        service = build("calendar", "v3", credentials=creds)

        event = {
            "summary": "Test Event2",
            "location": "Kiel",
            "description": "Test",
            "colorId": 4,
            "start": {
                "dateTime": "2024-10-04T09:00:00+02:00", # This is the format for the date and time, +02:00 is the timezone
                "timeZone": "Europe/Berlin",
            },
            "end": {
                "dateTime": "2024-10-04T17:00:00+02:00",
                "timeZone": "Europe/Berlin",
            },
            "recurrence": [
                "RRULE:FREQ=DAILY;COUNT=1"
            ],
            "attendees": [
                {"email": "heansuh.lee@student.fh-kiel.de"},
                {"email": "heansuh@gmail.com"}
            ]
        }

        event = service.events().insert(calendarId="primary", body=event).execute()

        print(f"Event created: {event.get('htmlLink')}")

        # now = dt.datetime.now().isoformat() + 'Z'

        # event_result = service.events().list(
        #     calendarId="primary",
        #     timeMin=now,
        #     maxResults=10,
        #     singleEvents=True,
        #     orderBy="startTime",
        # ).execute()

        # events = event_result.get("items", [])

        # if not events:
        #     print("No upcoming events found.")
        #     return
        
        # for event in events:
        #     start = event["start"].get("dateTime", event["start"].get("date"))
        #     print(start, event["summary"])

    except HttpError as error:
        print("An error occurred:", error)

if __name__ == "__main__":
    main()

# When the event has no title, it will not read the event.

