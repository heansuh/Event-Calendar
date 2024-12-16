# Disclaimer: The webpage of eventim actively blocks automatic scrapers and denies access if executed regularly. We therefore took the decision not to include this scraper anymore.
# While technically there would be ways to circumvent the blocking, we did not feel comfortable with implementing that due to potential legal constraints. 

# Scraper built by: Mareike Böckel and Flemming Reese
# Script for Eventim (Hamburg, Schleswig Holstein) Event Calendar
# The functions scrape_eventim() and preprocess_eventim() can be used for the main scraping process

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

def scrape_eventim(days_in_advance=30): # Optional parameter for how many days in advance to scrape events for

    # Preparing the date time frame to scrape according to days_in_advance parameter (getting current date and date x days in advance)
    today = datetime.today().date()
    today_plus_x = today + timedelta(days=days_in_advance)

    # Preparations for scraping (setting options, defining the url with the chosen timeframe, instantiating the Chrome driver, opening the website, defining waits)
    options = Options()
    options.add_argument("--headless")  
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")  # to avoid some rendering issues
    options.add_argument("--window-size=1920,1080")  # to avoid window size issues
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36") # to avoid being blocked as a headless scraper
    url = f"https://www.eventim.de/events/konzerte-1/?zipcode=24534&distance=100&shownonbookable=true&sort=DateAsc&dateFrom={today.year}-{today.month}-{today.day}&dateTo={today_plus_x.year}-{today_plus_x.month}-{today_plus_x.day}"
    driver = webdriver.Chrome(service=Service(), options=options)
    driver.get(url)
    time.sleep(5)
    wait = WebDriverWait(driver, 10)

    # Closing the cookie window
    try:
        element = driver.find_element(By.ID, "cmpwelcomebtnno")
        element.click()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    # Creating a list to store events
    extracted_info = [] 

    # Iterating over all pages of results (the loop is left when there uis no more "next page" to navigate to)
    while True:
        
        # Trying to find all events per page, the specific web element representing an event is identified via CSS selector, unfortunately process often blocked due to denied access by eventim (see disclaimer)
        try: 
            elements = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "product-group-item")))
            print(f"Number of elements found: {len(elements)}")
        except Exception as e:
            print(f"Error: {e}")
            driver.save_screenshot("screenshot.png")  # save screenshot for debugging (showed access denied due to being blocked)

        # Iterating over all found event elements and extracting the required information
        # As sometimes not all information is available per event, this part was made robust by using try-except statements per event and specifically for error prone description element (element doesn't always exist)
        for element in elements:
            try:
                title_element = element.find_element(By.CSS_SELECTOR, '[id^="listing-headline"]')
                title = title_element.text

                location_date_time_element = element.find_element(By.CSS_SELECTOR, ".text-overflow-ellipsis.u-text-color.theme-text-color")
                location_date_time = location_date_time_element.text

                try:
                    description_element = element.find_element(By.CSS_SELECTOR, ".listing-description.theme-text-color.text-overflow-ellipsis.hidden-xs")
                    description = description_element.text
                except Exception as e:
                    description = None

                source_element = element.find_element(By.CSS_SELECTOR, "a.btn.btn-sm.btn-block.btn-primary")
                source = source_element.get_attribute("href")

                # All information per event is stored into a dictionary and the dictionary is appended to the list of events
                extracted_info.append({
                    "Subject": title,
                    "Location_Date_Time": location_date_time,
                    "Description": description,
                    "Source": source,
                    "Category": "Konzert", # only concerts are scraped from this website
                    "Music_label": True # all scraped events from this website are music related
                })

            except Exception as e:
                continue
        
        # Navigate to the next page of results (by clicking on the next page button) if it exists, if not leave the loop
        try:
            pagination_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "pagination-item a[data-qa='nextPage']")))
            pagination_element.click()
            time.sleep(5)
        except Exception as e:
            print("No more pages")
            break

    # Last steps: Creating the dataframe of raw data from the event list, closing the driver and returning the dataframe
    df_raw = pd.DataFrame(extracted_info)
    driver.close()

    return df_raw


# Preprocessing function

def preprocess_eventim(df_raw):
    # Bringing the raw data into the agreed final data format (Seperating and preprocessing date, time and location information from the website with helper functions,
    # bringing together description elements, converting date format to YYYY-MM-DD, setting city as location (specifity of this website), dropping not further needed columns, 
    # changing column names into the agreed on final names, filling empties with " ", sorting the columns)
    df_raw[['Location', 'Date', 'Time']] = df_raw['Location_Date_Time'].apply(lambda x: pd.Series(split_location_date_time(x)))
    df_raw.drop(columns=['Location_Date_Time'], inplace=True)
    df_raw["Description"] = df_raw["Description"] + " " + df_raw["Source"]
    df_raw.drop(columns=['Source'], inplace=True)
    df_raw.rename(columns={'Time': 'Start_time'}, inplace=True)
    df_raw["End_time"] = " "
    df_raw['Date'] = df_raw['Date'].str[-10:]
    df_raw.rename(columns={'Date': 'Start_date'}, inplace=True)
    df_raw["Start_date"] = df_raw["Start_date"].apply(convert_date_format)
    df_raw["End_date"] = df_raw["Start_date"]
    df_raw["City"] = df_raw["Location"]
    df_raw = df_raw.fillna(" ")
    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

def split_location_date_time(value):
    # Preprocessing function to split the information on location, date and time given jointly on the website
    parts = value.split(',')
    if len(parts) == 3:
        return parts[0].strip(), parts[1].strip(), parts[2].strip()
    elif len(parts) == 2:
        return parts[0].strip(), parts[1].strip(), " "
    else:
        return value.strip(), " ", " "
    
def convert_date_format(date_str):
    # Function for converting date format from DD.MM.YYYY to YYYY-MM-DD 
    # Date format changed over the course of the project, so this function was added to older scrapers
    date_str = str(date_str)
    if "." in date_str:
        return pd.to_datetime(date_str, format='%d.%m.%Y').strftime('%Y-%m-%d')
    else:
        return " "
    

# Recommended usage of the above functions

df_raw = scrape_eventim(30) 
df_prep = preprocess_eventim(df_raw)
df_prep.to_csv("Scraped_Events_Eventim_HH_SH.csv")
print(df_prep.head())
print(df_prep.info())