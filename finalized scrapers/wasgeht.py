# Scraper built by: Heansuh Lee

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from datetime import datetime, timedelta
import pandas as pd
import time
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
    
def format_time(time_str):

    if pd.isna(time_str) != True:
        if "Uhr" in time_str:
            return time_str.replace(" Uhr", "")
    else:
        return None

def update_description(row):
    """
    Update the 'Description' column by appending additional information
    while filtering out 'nan' or 'N/A' values.
    """
    additional_info = ""
    if pd.notna(row['Event Details Link']):
        additional_info += f"{row['Event Details Link']}"
    return str(row['Description']) + additional_info
    # Return an empty string or handle missing values appropriately

def preprocessing(df):

    # Column changes
    df['Subject'] = df['Title']
    df['Start_date'] = df['Date']
    df['Start_time'] = df['Time'].apply(format_time)
    df['End_time'] = None
    df['Location'] = df['Location'].str.replace("pin", "").str.strip()
    df['Description'] = ""
    df['Description'] = df.apply(update_description, axis=1)

    # Drop unnecessary columns
    df.drop(columns=['Date','Title','Subtitle','Time','Location Link','Event Details Link'],inplace=True)

    return df

def wasgeht_scraper():
    # Initialize WebDriver
    
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=Service(), options=options)    

    # Open the target website
    url = "https://www.wasgehtapp.de/index.php?geo_id=22995&ort=Rendsburg&x=9.66986&y=54.3038&select_ort=1&radius=20&region=10"
    driver.get(url)

    time.sleep(5)

    # List of cities to scrape
    cities = ['Kiel', 'Lübeck', 'Flensburg', 'Neumünster', 'Rendsburg', 'Husum', 'Heide', 'Eckernförde', 'Schleswig', 'Itzehoe']

    # Today's date
    today = datetime.today()

    # Initialize data storage
    all_data = {}

    # Loop through each city
    for city in cities:
        city_data = []  # List to store data for this city

        # Select city input field and search for city
        try:
            city_input_button = driver.find_element(By.CSS_SELECTOR, "#select_ort")
            city_input_button.click()

            search_input = driver.find_element(By.CSS_SELECTOR, "#select_ort_input")
            search_input.send_keys(city)
            search_input.send_keys(Keys.RETURN)
            time.sleep(3)  # Wait for the page to reload
        except Exception as e:
            print(f"Error setting city {city} on {date_str}: {e}")
            continue

        # Loop through the next 10 days
        for day_offset in range(10):
            # Generate the URL for the specific day
            target_date = today + timedelta(days=day_offset)
            date_str = target_date.strftime("%Y-%m-%d")
            url = f"https://www.wasgehtapp.de/index.php?date={date_str}"
            driver.get(url)

            # Wait for the page to load
            time.sleep(5)

            # Locate all "katcontainer" containers
            try:
                containers = driver.find_elements(By.CSS_SELECTOR, ".katcontainer")
                # Exclude containers with the class "vorschau" or kat="kino"
                filtered_containers = [
                    container for container in containers
                    if "vorschau" not in container.get_attribute("class") and container.get_attribute("kat") != "kino"
                    ]       
            except Exception as e:
                print(f"Error fetching containers for city {city} on {date_str}: {e}")
                continue

            # Extract events data
            for container in filtered_containers:
                try:
                    category = container.get_attribute("kat")  # Get the "kat" attribute directly
                except:
                    category = 'Other'

                # Set Date as Target Date
                date = date_str

                # Find all events (termin) within this container
                events = container.find_elements(By.CSS_SELECTOR, ".termin")
                for event in events:
                    try:
                        title_element = event.find_element(By.CSS_SELECTOR, "h3.titel > a")
                        title = title_element.text.strip()
                        event_details_link = title_element.get_attribute("href")
                    except:
                        title = None
                        event_details_link = None

                    try:
                        subtitle = event.find_element(By.CSS_SELECTOR, ".subtitel").text.strip()
                    except:
                        subtitle = None

                    try:
                        time_start = event.find_element(By.CSS_SELECTOR, ".zeitloc > span.zeit").text.strip()
                    except:
                        time_start = None
                    try:
                        location_element = event.find_elements(By.CSS_SELECTOR, ".zeitloc > a")
                        if len(location_element) > 0:
                            location = location_element[0].text.strip()
                        else:
                            location = None

                        if len(location_element) > 1:
                            location_link = location_element[1].get_attribute("href")
                        else:
                            location_link = None
                    except:
                        location = None
                        location_link = None

                    # Append event data to city_data
                    city_data.append({
                        "Date": date,
                        "City": city,
                        "Category": category,
                        "Title": title,
                        "Subtitle": subtitle,
                        "Time": time_start,
                        "Location": location,
                        "Location Link": location_link,
                        "Event Details Link": event_details_link
                    })

        # Save data for this city to a CSV file
        city_df = pd.DataFrame(city_data)

        preprocessing(city_df)
        
        desired_columns = ['Subject', 'Start_date', 'Start_time', 'End_time', 'Location', 'City', 'Category', 'Description', 'Music_label']
        city_df = city_df.reindex(columns=desired_columns)
        city_df['Music_label'] = city_df['Category'].apply(lambda x: True if x in ['konzert', 'theater'] else False)

        city_df.to_csv(f"Scraped_Events_wasgeht_{city}.csv", index=False, encoding="utf-8")
        print(f"Data for {city} saved to Scraped_Events_wasgeht_{city}.csv")

    # Close the browser
    driver.quit()

wasgeht_scraper()