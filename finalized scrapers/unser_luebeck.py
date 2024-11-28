# Script for Scraping Unser Lübeck Event Calendar
# The functions scrape_unser_luebeck() and preprocess_unser_luebeck() should be imported to the script defining the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
import locale
from datetime import datetime
import time


# Scraping function

def scrape_unser_luebeck(days_in_advance=10): #events for today + 10 days in advance

    driver = webdriver.Firefox() #later switch to headless for automated scraping
    driver.get("https://www.unser-luebeck.de/veranstaltungskalender")
    time.sleep(3) #wait to fully load page

    # reject cookie window
    try:
        accept_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'div.jb-accept.btn.blue'))
        )
        accept_button.click()
    except Exception as e:
        print(f"An error occurred: {e}")

    # get currently processed date
    currently_processed_date = get_current_date(driver)

    # get events on that date 
    events_df = get_events_on_date(driver, currently_processed_date)
    
    for day in range(days_in_advance):

        #navigate to next day
        try:
            next_day_link = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[title="Folgetag"]'))
            )
            next_day_link.click()
        except Exception as e:
            print(f"An error occurred: {e}")

        # get currently processed date
        currently_processed_date = get_current_date(driver)

        # get events on that date 
        events_on_date = get_events_on_date(driver, currently_processed_date)

        events_df = pd.concat([events_df, events_on_date], axis=0)

    #end session
    time.sleep(5)
    driver.close()
    return events_df


# Preprocessing function

def preprocess_unser_luebeck(df_raw):

    df_raw.rename(columns={'Event': 'Subject'}, inplace=True)

    df_raw.rename(columns={'Date': 'Start_date'}, inplace=True)
    df_raw["End_date"] = df_raw["Start_date"]

    df_raw.rename(columns={'Time': 'Start_time'}, inplace=True)
    df_raw["End_time"] = "N/A"

    df_raw.rename(columns={'Source': 'Description'}, inplace=True)    

    df_raw['Music_label'] = df_raw['Category'].apply(check_music)

    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

def get_current_date(driver):

    # Set locale to German
    locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
    
    # aktuell angezeigtes Datum auslesen
    try:
        date_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.currentmonth'))
        )
        
        date_text = date_element.text.strip()
    except Exception as e:
        print(f"An error occurred: {e}")

    # Parse the date string
    date_obj = datetime.strptime(date_text, "%A, %d. %B %Y").date()
    return date_obj


def get_events_on_date(driver, currently_processed_date):

    # events auslesen
    try:
        ul_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.ev_ul'))
        )
        ul_text = ul_element.text
    except Exception as e:
        print(f"An error occurred: {e}")

    # events text preprocessen und in dictionary einordnen
    category_dict = {
        "Theater": [], 
        "Musik": [],
        "Film": [],
        "Ausstellungen": [],
        "Kunst":[],
        "Literatur": [],
        "Vorträge": [],
        "Sonstiges": []}
    
    event_list = ul_text.split('\n')
    
    for element in event_list:
        if element in category_dict.keys():
            current_cat = element
        else:
            category_dict[current_cat].append(element)

    for key in category_dict.keys():
        lst = category_dict[key]
        category_dict[key] = [tuple(lst[i:i+3]) for i in range(0, len(lst), 3)]
    
    #warum auch immer sind film infos anders geordnet
    films_prep = category_dict["Film"]
    category_dict["Film"] = []

    for event in films_prep:
        category_dict["Film"].append((event[-1], event[0], event[1]))

    # daten in df umwandeln und datum und source einfügen
    data_list = []
    for category, events in category_dict.items():
        for event in events:
            data_list.append((category, *event))

    df = pd.DataFrame(data_list, columns=['Category', 'Time', 'Event', 'Location'])
    df['City'] = 'Lübeck'
    df["Date"] = f'{currently_processed_date.day}.{currently_processed_date.month}.{currently_processed_date.year}'
    df['Source'] = f'https://www.unser-luebeck.de/veranstaltungskalender/eventsnachtag/{currently_processed_date.year}/{currently_processed_date.month}/{currently_processed_date.day}'#'https://www.unser-luebeck.de/veranstaltungskalender/'
    df = df[['Date', 'Event', 'Time', 'Location', 'City', 'Category', 'Source']]

    return df

def check_music(category):
    if category == "Musik":
        return 'music'
    return 'no music'


# Example usage

df_raw = scrape_unser_luebeck(10)
df_prep = preprocess_unser_luebeck(df_raw)
df_prep.to_csv("Scraped_Events_Unser_Luebeck.csv")