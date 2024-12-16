# Scraper built by: Mareike Böckel
# Script for Biunsinnorden (Hamburg, Schleswig Holstein) Event Calendar
# The functions scrape_biunsinnorden_sh_hh() and preprocess_biunsinnorden() can be used for the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import pandas as pd
import time


# Scraping function

def scrape_biunsinnorden_sh_hh():

    # Preparations for scraping (setting options, instantiating the Chrome driver, opening the website, defining waits, creating a list to store events)
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    url = "https://www.biunsinnorden.de/veranstaltungen/neumuenster/musik/umkreis-100"
    driver = webdriver.Chrome(service=Service(), options=options)
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    events_list = []
    time.sleep(5)

    # Closing the cookie window, if it appears (not always when using Chrome driver)
    try:
        button = driver.find_element(By.ID, 'cookie-accept-required')
        button.click()
    except Exception as e:
        print(e)

    # Navigating through the first eight pages of results (8 was chosen as a heuristic for covering an appropriate timeframe)
    # Navigation choice: Varying the URL to use it as an api to navigate through pages (proved as more stable than clicking on "next" button)
    for i in range(1,9):
        url = f"https://www.biunsinnorden.de/veranstaltungen/neumuenster/musik/umkreis-100?Page={i}#Termine"
        driver.get(url)
        time.sleep(5)
        
        # Finding all events per page, the specific web element representing an event is identified via CSS selector
        event_elements = driver.find_elements(By.CSS_SELECTOR, 'div.row[itemscope][itemtype="http://schema.org/Event"]')

        for event in event_elements:

            # Sometimes no start time is given, so the retrieval of this information had to be made more robust
            try:
                start_time = event.find_element(By.CSS_SELECTOR, '.time.standard').text[:-4]
            except Exception as e:
                start_time = " "

            # All information per event is stored into a dictionary and the dictionary is appended to the list of events
            event_info = {
                'Start_date': event.find_element(By.CSS_SELECTOR, 'meta[itemprop="startDate"]').get_attribute('content')[:-6],
                'End_date': event.find_element(By.CSS_SELECTOR, 'meta[itemprop="startDate"]').get_attribute('content')[:-6],
                'Start_time': start_time,
                'End_time': " ",
                'Subject': event.find_element(By.CSS_SELECTOR, '.title a').get_attribute('title'),
                'Category': event.find_element(By.CSS_SELECTOR, '.category').text,
                'Music_label': True, # All events on this website are music related
                'Location': event.find_element(By.CSS_SELECTOR, '.venue a').get_attribute('title'),
                'City': event.find_element(By.CSS_SELECTOR, '.city span[itemprop="addressLocality"]').text,
                'Description': event.find_element(By.CSS_SELECTOR, '.title a').get_attribute('href')
            }
            events_list.append(event_info)

    # Last steps: Creating the dataframe of raw data from the event list, closing the driver and returning the dataframe
    df_raw = pd.DataFrame(events_list)
    driver.close()

    return df_raw   


# Preprocessing function

def preprocess_biunsinnorden(df_raw):
    # Bringing the raw data into the agreed final data format (Lowercasing city names except for first letters, filling empties with " ", sorting the columns)
    df_raw['City'] = df_raw['City'].str.title()
    df_raw = df_raw.fillna(" ")
    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Recommended usage of the above functions

df_raw = scrape_biunsinnorden_sh_hh()
df_prep = preprocess_biunsinnorden(df_raw)
df_prep.to_csv("Scraped_Events_BiUnsInNorden_SH_HH.csv")
print(df_prep.head())
print(df_prep.info())