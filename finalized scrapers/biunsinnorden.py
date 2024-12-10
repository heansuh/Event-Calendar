# Script for Live Gigs (Hamburg, Schleswig Holstein) Event Calendar
# The functions scrape_biunsinnorden_sh_hh() and preprocess_biunsinnorden() should be imported to the script defining the main scraping process

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

def scrape_biunsinnorden_sh_hh():

    # preparations
    options = Options()
    options.add_argument("--headless")  # Run Chromium in headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    url = "https://www.biunsinnorden.de/veranstaltungen/neumuenster/musik/umkreis-100"
    #driver = webdriver.Firefox()
    driver = webdriver.Chrome(service=Service(), options=options)
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    events_list = []
    time.sleep(5)

    #cookie rejection (unter Chrome ggf mehrfach nötig TODO, scheint aber scraping prozess nicht zu stören)
    try:
        button = driver.find_element(By.ID, 'cookie-accept-required')
        button.click()
    except Exception as e:
        print(e)

    for i in range(1,9): #über 8 seiten blättern (erst mit klick auf nächste aber problem mit werbebannern, jetzt url als api)
        url = f"https://www.biunsinnorden.de/veranstaltungen/neumuenster/musik/umkreis-100?Page={i}#Termine"
        driver.get(url)
        time.sleep(5)
        event_elements = driver.find_elements(By.CSS_SELECTOR, 'div.row[itemscope][itemtype="http://schema.org/Event"]')

        for event in event_elements:

            # sometimes no start time is given
            try:
                start_time = event.find_element(By.CSS_SELECTOR, '.time.standard').text[:-4]
            except Exception as e:
                start_time = " "

            #collect rest of the info
            event_info = {
                'Start_date': event.find_element(By.CSS_SELECTOR, 'meta[itemprop="startDate"]').get_attribute('content')[:-6],
                'End_date': event.find_element(By.CSS_SELECTOR, 'meta[itemprop="startDate"]').get_attribute('content')[:-6],
                'Start_time': start_time,
                'End_time': " ",
                'Subject': event.find_element(By.CSS_SELECTOR, '.title a').get_attribute('title'),
                'Category': event.find_element(By.CSS_SELECTOR, '.category').text,
                'Music_label': True,
                'Location': event.find_element(By.CSS_SELECTOR, '.venue a').get_attribute('title'),
                'City': event.find_element(By.CSS_SELECTOR, '.city span[itemprop="addressLocality"]').text,
                'Description': event.find_element(By.CSS_SELECTOR, '.title a').get_attribute('href')
            }
            events_list.append(event_info)

    df_raw = pd.DataFrame(events_list)
    driver.close()

    return df_raw   


# Preprocessing function

def preprocess_biunsinnorden(df_raw):
    df_raw['City'] = df_raw['City'].str.title()
    df_raw = df_raw.fillna(" ")
    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Example usage

df_raw = scrape_biunsinnorden_sh_hh()
df_prep = preprocess_biunsinnorden(df_raw)
df_prep.to_csv("Scraped_Events_BiUnsInNorden_SH_HH.csv")
print(df_prep.head())
print(df_prep.info())