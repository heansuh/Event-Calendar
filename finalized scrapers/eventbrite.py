# Scraper built by: Mareike Böckel
# Script for Scraping Eventbrite Event Calendar
# The functions scrape_eventbrite_hh_sh() and preprocess_eventbrite() can be used for the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import pandas as pd
import time
from datetime import datetime, timedelta


# Scraping function

def scrape_eventbrite_hh_sh():

    # Preparations for scraping (setting options, instantiating the Chrome driver, specifying the urls to scrape, creating a dataframe to save the event data in)
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    urls = ["https://www.eventbrite.de/d/germany--hamburg/music--events--this-month/?page=1",
        "https://www.eventbrite.de/d/germany--hamburg/music--events--next-month/?page=1",
        "https://www.eventbrite.de/d/germany--schleswig-holstein/music--events--this-month/?page=1",
        "https://www.eventbrite.de/d/germany--schleswig-holstein/music--events--next-month/?page=1"]
    driver = webdriver.Chrome(service=Service(), options=options)
    df_raw = pd.DataFrame()

    # Iterating over the urls to scrape (urls are used as an api to stably navigate through the website regarding switching location and timeframe)
    for url in urls:

        # Opening each url and getting the event information from that page (with helper function)
        driver.get(url)
        events_df = get_events_on_page(driver)
        df_raw = pd.concat([df_raw, events_df], ignore_index=True)

        # Navigating through the pages if results per location and timeframe choice have several pages by modifying the page number specified in the url
        try: 
            pagination_element = driver.find_element(By.CSS_SELECTOR, '.Pagination-module__search-pagination__navigation-minimal___1eHd9')
            pagination_text = pagination_element.text
            max_pages = int(pagination_text[-1])
            if max_pages > 1:
                further_urls = []
                for i in range (2,max_pages+1):
                    further_urls.append(url[:-1] + str(i))
                # Opening each new url and getting the event information from that page (with helper function)
                for furl in further_urls:
                    driver.get(furl)
                    events_df = get_events_on_page(driver)
                    df_raw = pd.concat([df_raw, events_df], ignore_index=True)
        except Exception as e:
            continue
    # Last steps: Closing the driver and returning the dataframe of raw event data
    driver.close()

    return df_raw


# Preprocessing function

def preprocess_eventbrite(df_raw):
    # Bringing the raw data into the agreed final data format (Processing and seperating date and time, changing column names into the agreed on final names,
    # converting final date format from DD.MM.YYYY to YYYY-MM-DD, checking the category information for music relatedness and adding music label True or False (with helper function),
    # dropping not further needed columns, filling empties with " ", sorting the columns)
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
    # Helper function to retrieve all event information per page by extracting the information from the individual webelements related to the required information
    time.sleep(5)

    # Finding all events per page, the specific web element representing an event is identified via class name 
    event_cards = driver.find_elements(By.CLASS_NAME, 'event-card-details')

    # List to store extracted event information
    events = []

    # Iterating over all found event elements and extracting the required information from attributes and textual elements of specific web elements and storing them in dictionaries
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
    
    # Handing back the dataframe of events found on this page to the scraping function defined above
    return pd.DataFrame(events)

def check_music_label(label):
    # Function to check if an event is music related or not according to its category and provide the correct label
    if 'music' in label.lower():
        return True
    else:
        return False
    
def extract_and_reformat_date(date_str):
    # Processing date information found on webpage into DD.MM.
    parts = date_str.split(',')
    if len(parts) >= 3:
        date_part = parts[1].strip()
        day, month = date_part.split()
        month_map = {
            'Jan.': '01', 'Feb.': '02', 'Mär.': '03', 'Apr.': '04',
            'Mai': '05', 'Jun.': '06', 'Jul.': '07', 'Aug.': '08',
            'Sep.': '09', 'Okt.': '10', 'Nov.': '11', 'Dez.': '12'
        }
        day = day.zfill(2)
        month = month_map.get(month, month)
        return f"{day}{month}."
    if "um" in date_str:
        return date_str.split()[0]
    return date_str

def parse_relative_date(date_str):
    # Processing of cases with relative date information (today/tomorrow) into DD.MM.
    today = datetime.today()
    if 'heute' in date_str:
        return today.strftime('%d.%m.')
    elif 'morgen' in date_str:
        return (today + timedelta(days=1)).strftime('%d.%m.')
    else:
        return date_str
    
# Function to convert date from DD.MM. to YYYY-MM-DD
def convert_date(date_str):
    # Function for converting date format from DD.MM to YYYY-MM-DD 
    # For this the year, which was not specified in the event details, is imputed by assuming the event is on the next possible date (scraping for this+next month allows for this assumption)
    if "." in date_str:
        current_date = datetime.now()
        day, month = map(int, date_str.strip('.').split('.'))
        next_year = current_date.year if (month > current_date.month or (month == current_date.month and day >= current_date.day)) else current_date.year + 1
        new_date = datetime(next_year, month, day)
        return new_date.strftime('%Y-%m-%d')
    else:
        return " "
    

# Recommended usage of the above functions

df_raw = scrape_eventbrite_hh_sh()
df_prep = preprocess_eventbrite(df_raw)
df_prep.to_csv("Scraped_Events_HH_SH_Eventbrite.csv")
print(df_prep.head())
print(df_prep.info())