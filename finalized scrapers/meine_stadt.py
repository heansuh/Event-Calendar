# Script for Meine Stadt (HH, SH) Event Calendar
# The functions scrape_meine_stadt() and preprocess_meine_stadt() should be imported to the script defining the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import pandas as pd
from datetime import datetime
import time


# Scraping function

def scrape_meine_stadt(): # Small warning: one execution takes >30 minutes, as there are many suitable events and locations on this website

    # Defining the urls to scrape, varying location and category to use the url as an api to the website
    urls = [
        "https://veranstaltungen.meinestadt.de/hamburg/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/hamburg/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/hamburg/festivals/alle",
        "https://veranstaltungen.meinestadt.de/kiel/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/kiel/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/kiel/festivals/alle",
        "https://veranstaltungen.meinestadt.de/luebeck/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/luebeck/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/luebeck/festivals/alle",
        "https://veranstaltungen.meinestadt.de/flensburg/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/flensburg/partys-feiern/alle", 
        "https://veranstaltungen.meinestadt.de/flensburg/festivals/alle",
        "https://veranstaltungen.meinestadt.de/husum-nordsee/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/husum-nordsee/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/husum-nordsee/festivals/alle",
        "https://veranstaltungen.meinestadt.de/heide/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/heide/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/heide/festivals/alle",
        "https://veranstaltungen.meinestadt.de/schleswig/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/schleswig/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/schleswig/festivals/alle",
        "https://veranstaltungen.meinestadt.de/itzehoe/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/itzehoe/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/itzehoe/festivals/alle"
    ] 
    
    # Preparations for scraping (setting options (more than before due to prior difficulties with headless mode on this website), instantiating the Chrome driver, defining waits)
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")  # to avoid some rendering issues
    options.add_argument("--window-size=1920,1080")  # to avoid window size issues
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36") # to avoid being blocked as a headless scraper
    options.add_argument("--log-level=3")  # to suppress most logs, as many unimportant ones appear here
    driver = webdriver.Chrome(service=Service(), options=options)
    wait = WebDriverWait(driver, 10) 

    # Creating a list to store events
    events = []

    # Iterating over the different urls to scrape and opening each of them
    for url in urls:
        driver.get(url)

        # Closing the cookie window, if it appears
        try:
            iframe = wait.until(EC.presence_of_element_located((By.ID, "sp_message_iframe_1220563")))
            driver.switch_to.frame(iframe)
            buttons = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "button-responsive-primary")))
            buttons[1].click()
        except Exception as e:
            print("Cookie rejection in iframe didn't work. No problem for scraping.")

        # Moving back from cookie iframe to actual website
        driver.switch_to.default_content()

        # Heuristic of how often to click on load more events button in order to minimally cover the proper timeframe also for location and category combinations with many events
        # If not more events can be loaded the loop is left 
        for i in range(30): 
            time.sleep(10) # This website takes especially long to load properly
            try: 
                wait = WebDriverWait(driver, 10)
                load_more_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@data-component="CsSecondaryButton"]')))
                load_more_button.click()
            except Exception as e:
                print("No further events to load.")
                break

        # Finding all events per page, the specific web element representing an event is identified via CSS selector
        elements = driver.find_elements(By.CSS_SELECTOR, 'div.flex.flex-col.w-full.p-16.screen-m\\:pl-0')
        print("Found events:", str(len(elements)))

        # Iterating over all found event elements and extracting the required information
        # As sometimes not all information is available per event, this part was made robust by using try-except statements
        for element in elements:
            
            try:
                title = element.find_element(By.CSS_SELECTOR, 'h3.text-h3.font-bold-headline.mb-8.line-clamp-2').text
            except:
                title = ' '
            
            try:
                source = element.find_element(By.CSS_SELECTOR, 'a.ms-clickArea').get_attribute('href')
            except:
                source = ' '
            
            try:
                date_time = element.find_element(By.CSS_SELECTOR, 'div.flex.mb-4.text-h4').text
            except:
                date_time = ' '
            
            try:
                city_location = element.find_element(By.CSS_SELECTOR, 'div.flex.mb-8.text-h4').text
            except:
                city_location = ' '
            
            # All information per event is stored into a dictionary and the dictionary is appended to the list of events
            event = {
                "Subject": title,
                "Description": source, 
                "Date_Time": date_time,
                "City_Location": city_location,
                "Category": url.split(('/'))[-2],
                "Music_label": True # all scraped events from this website are music related
            }
            events.append(event)

    # Last steps: Creating the dataframe of raw data from the event list, closing the driver and returning the dataframe
    df_raw = pd.DataFrame(events)
    driver.close()

    return df_raw


# Preprocessing function

def preprocess_meine_stadt(df_raw):
    # Bringing the raw data into the agreed final data format (SEperating and preprocessing date, time, city and location information from the website with helper functions,
    # converting date format to YYYY-MM-DD, dropping not further needed columns, filling empties with " ", sorting the columns)
    df_raw["Start_date"] = df_raw["Date_Time"].apply(preprocess_date)
    df_raw["Start_time"] = df_raw["Date_Time"].apply(preprocess_time)
    df_raw["Start_date"] = df_raw["Start_date"].apply(convert_date_format)
    df_raw["End_date"] = df_raw["Start_date"]
    df_raw["End_time"] = " "
    df_raw.drop(columns=["Date_Time"], inplace= True)
    df_raw["City"] = df_raw["City_Location"].apply(preprocess_city)
    df_raw["Location"] = df_raw["City_Location"].apply(preprocess_location)
    df_raw.drop(columns=["City_Location"], inplace= True)
    df_raw = df_raw.fillna(" ")
    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

def preprocess_date(date_time_string):
    # Extracting date information if given in regular format
    if len(date_time_string.split(" ")) > 1:
        date = date_time_string.split(" ")[1][:-1]
    else:
        date = " "
    return date

def preprocess_time(date_time_string):
    # Extracting time information if given in regular format
    if len(date_time_string.split(" ")) > 2:
        time = date_time_string.split(" ")[2]
    else:
        time = " "
    return time

def convert_date_format(date_str):
    # Function for converting date format from DD.MM.YYYY to YYYY-MM-DD 
    # Date format changed over the course of the project, so this function was added to older scrapers
    date_str = str(date_str)
    if "." in date_str:
        return pd.to_datetime(date_str, format='%d.%m.%Y').strftime('%Y-%m-%d')
    else:
        return " "

def preprocess_city(citlocstr):
    # Extracting city information if given in regular format
    city = citlocstr.split(", ", 1)[0]
    return city

def preprocess_location(citlocstr):
    # Extracting location information if given in regular format
    if len(citlocstr.split(", ", 1)) == 2:
        location = citlocstr.split(", ", 1)[1]
    else:
        location = " "
    return location


# Recommended usage of the above functions

df_raw = scrape_meine_stadt()
df_prep = preprocess_meine_stadt(df_raw)
df_prep.to_csv("Scraped_Events_Meine_Stadt.csv")
print(df_prep.head())
print(df_prep.info())
