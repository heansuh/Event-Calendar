# Script for Live Gigs (Hamburg, Schleswig Holstein) Event Calendar
# The functions scrape_live_gigs_hh_sh() and preprocess_live_gigs() can be used for the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import pandas as pd
import time


# Scraping function

def scrape_live_gigs_hh_sh():

    # Preparations for scraping (setting options, instantiating the Chrome driver, opening the website, defining waits)
    options = Options()
    options.add_argument("--headless")  
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    url = "https://www.livegigs.de/neumuenster/umkreis-100#Termine"
    driver = webdriver.Chrome(service=Service(), options=options)
    driver.get(url)
    wait = WebDriverWait(driver, 10)

    # Closing the cookie window
    try:
        button = wait.until(EC.element_to_be_clickable((By.ID, 'cookie-accept-required')))
        button.click()
    except Exception as e:
        print(f"An error occurred: {e}")

    # Creating a list to store events
    events_data = []

    # Navigating through the first two pages of results (2 was chosen as a heuristic for covering an appropriate timeframe)
    # Navigation choice: Clicking on the "next" button proved as a stable navigation option
    for i in range(2):

        # Finding all events per page, the specific web element representing an event is identified via class name 
        elements = driver.find_elements(By.CLASS_NAME, 'box-eventline')

        # Iterating over all found event elements and extracting the required information
        # As sometimes not all information is available per event, this part was made robust by using try-except statements
        # Elements that have no title are skipped right away, if other information is missing, the fields are left empty at first
        for element in elements:
            try:
                title = element.find_element(By.CLASS_NAME, 'summary').get_attribute('title')
                title = title.split(" - ")[0]
            except:
                continue

            try:
                source = element.find_element(By.CLASS_NAME, 'summary').get_attribute('href')
            except:
                source = None

            try:
                time_standard = element.find_element(By.CLASS_NAME, 'time').text[:6].strip()
            except:
                time_standard = None
            
            try:
                day = element.find_element(By.CLASS_NAME, 'day').text
                month = element.find_element(By.CLASS_NAME, 'month').get_attribute('title').split('-')[1]
                year = element.find_element(By.CLASS_NAME, 'year').text
                formatted_date = f"{day}.{month}.{year}"
            except:
                formatted_date = None

            try:
                category = element.find_element(By.CLASS_NAME, 'category').text
            except:
                category = None

            try:
                location = element.find_element(By.CLASS_NAME, 'venue').get_attribute('title')
            except:
                location = None

            try:
                city = element.find_element(By.CLASS_NAME, 'city').text
            except:
                city = None

            # All information per event is stored into a dictionary and the dictionary is appended to the list of events
            events_data.append({
                'Subject': title,
                'Description': source,
                'Start_time': time_standard,
                'End_time': " ",
                'Start_date': formatted_date,
                'End_date': formatted_date,
                'Category': category,
                'Location': location,
                'City': city,
                'Music_label': True # All events on this website are music related
            })

        # Navigating to page 2 (this generally covers at minimum the events of the next month)
        if i < 1:
            try:
                next_day_link = driver.find_element(By.XPATH, '//div[@class="standard link-text"]/a[contains(text(), "nächster Tag")]')
                next_day_link.click()
                time.sleep(5)
            except Exception as e:
                print("No second page exists.")
                break
    
    # Last steps: Creating the dataframe of raw data from the event list, closing the driver and returning the dataframe
    df_raw = pd.DataFrame(events_data)
    driver.close()

    return df_raw


# Preprocessing function

def preprocess_live_gigs(df_raw):
    # Bringing the raw data into the agreed final data format (Converting date format from DD.MM.YYYY to YYYY-MM-DD,
    # lowercasing city names except for first letters, filling empties with " ", sorting the columns)
    df_raw["Start_date"] = df_raw["Start_date"].apply(convert_date_format)
    df_raw["End_date"] = df_raw["End_date"].apply(convert_date_format)
    df_raw['City'] = df_raw['City'].str.title()
    df_raw = df_raw.fillna(" ")
    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

def convert_date_format(date_str):
    # Function for converting date format from DD.MM.YYYY to YYYY-MM-DD 
    # Date format changed over the course of the project, so this function was added to older scrapers
    date_str = str(date_str)
    if "." in date_str:
        return pd.to_datetime(date_str, format='%d.%m.%Y').strftime('%Y-%m-%d')
    else:
        return " "


# Recommended usage of the above functions

df_raw = scrape_live_gigs_hh_sh()
df_prep = preprocess_live_gigs(df_raw)
df_prep.to_csv("Scraped_Events_Live_Gigs_HH_SH.csv")
print(df_prep.head())
print(df_prep.info())