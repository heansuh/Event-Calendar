# Script for Scraping Unser Lübeck Event Calendar
# The functions scrape_unser_luebeck() and preprocess_unser_luebeck() can be used for the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import pandas as pd
import locale
from datetime import datetime
import time


# Scraping function

def scrape_unser_luebeck(days_in_advance=10): # Optional parameter for how many days in advance to scrape events for

    # Preparations for scraping (setting options, instantiating the Chrome driver, opening the website)
    options = Options()
    options.add_argument("--headless") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(), options=options)
    driver.get("https://www.unser-luebeck.de/veranstaltungskalender")
    time.sleep(3) 

    # Closing the cookie window
    try:
        accept_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'div.jb-accept.btn.blue'))
        )
        accept_button.click()
    except Exception as e:
        print(f"An error occurred: {e}")

    # Getting the currently processed date (with helper function)
    currently_processed_date = get_current_date(driver)

    # Getting all events on that date (with helper function)
    events_df = get_events_on_date(driver, currently_processed_date)
    
    # Iterating over the number of days to scrape by navigating through the dates (one page per date)
    # Navigation choice: Clicking on the "following day" button proved as a stable navigation option
    for day in range(days_in_advance):

        try:
            next_day_link = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[title="Folgetag"]'))
            )
            next_day_link.click()
        except Exception as e:
            print(f"An error occurred: {e}")

        # Getting the new currently processed date (with helper function)
        currently_processed_date = get_current_date(driver)

        # Getting all events on that date (with helper function) and adding them to the raw dataframe of events
        events_on_date = get_events_on_date(driver, currently_processed_date)
        events_df = pd.concat([events_df, events_on_date], axis=0)

    # Last steps: Closing the driver and returning the dataframe
    driver.close()

    return events_df


# Preprocessing function

def preprocess_unser_luebeck(df_raw):
    # Bringing the raw data into the agreed final data format (Changing column names into the agreed on final names, converting date format from DD.MM.YYYY to YYYY-MM-DD,
    # checking the category information for music relatedness and adding music label True or False (with helper function), filling empties with " ", sorting the columns)
    df_raw.rename(columns={'Event': 'Subject'}, inplace=True)

    df_raw.rename(columns={'Date': 'Start_date'}, inplace=True)
    df_raw["Start_date"] = df_raw["Start_date"].apply(convert_date_format)
    df_raw["End_date"] = df_raw["Start_date"]

    df_raw.rename(columns={'Time': 'Start_time'}, inplace=True)
    df_raw["End_time"] = " "

    df_raw.rename(columns={'Source': 'Description'}, inplace=True)    

    df_raw['Music_label'] = df_raw['Category'].apply(check_music)

    df_raw = df_raw.fillna(" ")

    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

def get_current_date(driver):
    # Function to read the date that the current information on the website is related to
    locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8') # Setting locale to German for correct interpretation of the information
    
    try:
        date_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.currentmonth'))
        )
        
        date_text = date_element.text.strip()
    except Exception as e:
        print(f"An error occurred: {e}")

    date_obj = datetime.strptime(date_text, "%A, %d. %B %Y").date()
    return date_obj


def get_events_on_date(driver, currently_processed_date):

    # Helper function to retrieve all event information per page by extracting the textual elements and processing them
    try:
        ul_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.ev_ul'))
        )
        ul_text = ul_element.text
    except Exception as e:
        print(f"An error occurred: {e}")

    # Dictionary to store events per category 
    category_dict = {
        "Theater": [], 
        "Musik": [],
        "Film": [],
        "Ausstellungen": [],
        "Kunst":[],
        "Literatur": [],
        "Vorträge": [],
        "Sonstiges": []}
    
    # Splitting textual information into individual pieces of info
    event_list = ul_text.split('\n')
    
    # Iterating over the found information, recognizing the correct category and storing event information into it
    for element in event_list:
        if element in category_dict.keys():
            current_cat = element
        else:
            category_dict[current_cat].append(element)

    # Processing information per event in tuples
    for key in category_dict.keys():
        lst = category_dict[key]
        category_dict[key] = [tuple(lst[i:i+3]) for i in range(0, len(lst), 3)]
    
    # Reorganizing movie related event information as the info on this type of event is structured differently
    films_prep = category_dict["Film"]
    category_dict["Film"] = []
    for event in films_prep:
        category_dict["Film"].append((event[-1], event[0], event[1]))

    # Creating dataframe of raw event data from the retrieved information, adding columns with city, date and source information
    data_list = []
    for category, events in category_dict.items():
        for event in events:
            data_list.append((category, *event))
    df = pd.DataFrame(data_list, columns=['Category', 'Time', 'Event', 'Location'])
    df['City'] = 'Lübeck'
    df["Date"] = f'{currently_processed_date.day}.{currently_processed_date.month}.{currently_processed_date.year}'
    df['Source'] = f'https://www.unser-luebeck.de/veranstaltungskalender/eventsnachtag/{currently_processed_date.year}/{currently_processed_date.month}/{currently_processed_date.day}'
    df = df[['Date', 'Event', 'Time', 'Location', 'City', 'Category', 'Source']]

    # Handing back the dataframe of events found on this page to the scraping function defined above
    return df

def check_music(category):
    # Function to check if an event is music related or not according to its category and provide the correct label
    if category == "Musik":
        return True
    return False

def convert_date_format(date_str):
    # Function for converting date format from DD.MM.YYYY to YYYY-MM-DD 
    # Date format changed over the course of the project, so this function was added to older scrapers
    date_str = str(date_str)
    if "." in date_str:
        return pd.to_datetime(date_str, format='%d.%m.%Y').strftime('%Y-%m-%d')
    else:
        return " "


# Recommended usage of the above functions

df_raw = scrape_unser_luebeck(10)
df_prep = preprocess_unser_luebeck(df_raw)
df_prep.to_csv("Scraped_Events_Unser_Luebeck.csv")
print(df_prep.head())
print(df_prep.info())