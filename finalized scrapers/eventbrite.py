# Script for Scraping Eventbrite Event Calendar
# The functions scrape_eventbrite_hh_sh() and preprocess_eventbrite() should be imported to the script defining the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import pandas as pd
import time
from datetime import datetime, timedelta


# Scraping function

def scrape_eventbrite_hh_sh():

    # Preparations
    options = Options()
    options.add_argument("--headless")  # Run Chromium in headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    urls = ["https://www.eventbrite.de/d/germany--hamburg/music--events--this-month/?page=1",
        "https://www.eventbrite.de/d/germany--hamburg/music--events--next-month/?page=1",
        "https://www.eventbrite.de/d/germany--schleswig-holstein/music--events--this-month/?page=1",
        "https://www.eventbrite.de/d/germany--schleswig-holstein/music--events--next-month/?page=1"]
    
    # Initialize the driver with Chromium
    driver = webdriver.Chrome(service=Service(), options=options)
    # driver.get(urls)

    df_raw = pd.DataFrame()

    for url in urls:

        driver.get(url)
        events_df = get_events_on_page(driver)
        df_raw = pd.concat([df_raw, events_df], ignore_index=True)

        try: 
            pagination_element = driver.find_element(By.CSS_SELECTOR, '.Pagination-module__search-pagination__navigation-minimal___1eHd9')
            pagination_text = pagination_element.text
            max_pages = int(pagination_text[-1])
            if max_pages > 1:
                further_urls = []
                for i in range (2,max_pages+1):
                    further_urls.append(url[:-1] + str(i))
                for furl in further_urls:
                    driver.get(furl)
                    events_df = get_events_on_page(driver)
                    df_raw = pd.concat([df_raw, events_df], ignore_index=True)
        except Exception as e:
            continue

    driver.close()

    return df_raw


# Preprocessing function

def preprocess_eventbrite(df_raw):

    df_raw.loc[df_raw['Date and time'].str.contains(r'\s\+\s\d+\smore'), 'Date and time'] = df_raw['Date and time'].str[:-9]
    
    df_raw['Start_time'] = df_raw['Date and time'].str[-5:]
    
    df_raw['Date formated'] = df_raw['Date and time'].apply(parse_relative_date)
    df_raw['Date'] = df_raw['Date formated'].apply(extract_and_reformat_date)

    df_raw.rename(columns={'Title': 'Subject'}, inplace=True)

    df_raw.rename(columns={'Source': 'Description'}, inplace=True) 

    df_raw["Category"] = df_raw["Music_label"]

    df_raw.rename(columns={'Date': 'Start_date'}, inplace=True)
    df_raw['Start_date'] = df_raw['Start_date'].apply(convert_date)
    df_raw["End_date"] = df_raw["Start_date"]
    
    df_raw.drop(columns=['Date and time', 'Date formated'], inplace=True)

    df_raw["End_time"] = " "

    df_raw['Music_label'] = df_raw['Music_label'].apply(check_music_label)

    df_raw = df_raw.fillna(" ")

    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

def get_events_on_page(driver):

    time.sleep(5)

    event_cards = driver.find_elements(By.CLASS_NAME, 'event-card-details')

    # List to store extracted event details
    events = []

    # Loop through each event card and extract details
    for card in event_cards:
        try:
            title = card.find_element(By.CLASS_NAME, 'event-card-link').get_attribute('aria-label')
            source = card.find_element(By.CLASS_NAME, 'event-card-link').get_attribute('href')
            city = card.find_element(By.CLASS_NAME, 'event-card-link').get_attribute('data-event-location')
            music_label = card.find_element(By.CLASS_NAME, 'event-card-link').get_attribute('data-event-category')
            date_time = card.find_element(By.XPATH, ".//p[contains(@class, 'event-card__clamp-line--one')][1]").text
            location = card.find_element(By.XPATH, ".//p[contains(@class, 'event-card__clamp-line--one')][2]").text
            
            events.append({
                'Title': title,
                'Source': source,
                'City': city,
                'Music_label': music_label,
                'Date and time': date_time,
                'Location': location
            })
        except Exception as e:
            continue
    
    return pd.DataFrame(events)

def check_music_label(label):
    # Check if the string contains the word "music"
    if 'music' in label.lower():
        return True
    else:
        return False
    
def extract_and_reformat_date(date_str):
    parts = date_str.split(',')
    if len(parts) >= 3:
        date_part = parts[1].strip()
        day, month = date_part.split()
        month_map = {
            'Jan.': '01', 'Feb.': '02', 'Mär.': '03', 'Apr.': '04',
            'Mai.': '05', 'Jun.': '06', 'Jul.': '07', 'Aug.': '08',
            'Sep.': '09', 'Okt.': '10', 'Nov.': '11', 'Dez.': '12'
        }
        day = day.zfill(2)
        month = month_map.get(month, month)
        return f"{day}{month}."
    if "um" in date_str:
        return date_str.split()[0]
    return date_str

def parse_relative_date(date_str):
    today = datetime.today()
    if 'heute' in date_str:
        return today.strftime('%d.%m.') #('%d.%m.%Y')
    elif 'morgen' in date_str:
        return (today + timedelta(days=1)).strftime('%d.%m.') #%Y')
    else:
        return date_str
    
# Function to convert date from DD.MM. to YYYY-MM-DD
def convert_date(date_str):
    
    if "." in date_str:
        # Get current date
        current_date = datetime.now()
        
        # Extract day and month from date_str, ensuring zero-padding
        day, month = map(int, date_str.strip('.').split('.'))
        
        # Determine next possible year
        next_year = current_date.year if (month > current_date.month or (month == current_date.month and day >= current_date.day)) else current_date.year + 1
        
        # Create a new date object
        new_date = datetime(next_year, month, day)
        
        # Return the formatted date string
        return new_date.strftime('%Y-%m-%d')
    else:
        return " "
    

# Example usage

df_raw = scrape_eventbrite_hh_sh()
df_prep = preprocess_eventbrite(df_raw)
df_prep.to_csv("Scraped_Events_HH_SH_Eventbrite.csv")