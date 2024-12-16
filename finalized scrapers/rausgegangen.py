# Script for Rausgegangen.de (Hamburg, Kiel, Lübeck, Flensburg) Event Calendar
# The functions scrape_rausgegangen_hh_ki_hl_fl() and preprocess_rausgegangen() can be used for the main scraping process

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

def scrape_rausgegangen_hh_ki_hl_fl():

    # Defining the urls to scrape, varying location and category to use the url as an api to the website
    # For each url specifying city and category and pages to loop through explicitly
    scraping = [
        ("https://rausgegangen.de/hamburg/kategorie/konzerte-und-musik/", 'Hamburg', 'Konzerte & Musik', 5), 
        ("https://rausgegangen.de/hamburg/kategorie/party/", 'Hamburg', 'Party', 1),
        ("https://rausgegangen.de/hamburg/kategorie/feste-und-festival/", 'Hamburg', 'Feste & Festival', 1),
        ("https://rausgegangen.de/kiel/kategorie/konzerte-und-musik/", 'Kiel', 'Konzerte & Musik', 1),
        ("https://rausgegangen.de/kiel/kategorie/party/", 'Kiel', 'Party', 1),
        ("https://rausgegangen.de/kiel/kategorie/feste-und-festival/", 'Kiel', 'Feste & Festival', 1),
        ("https://rausgegangen.de/lubeck/kategorie/konzerte-und-musik/", 'Lübeck', 'Konzerte & Musik', 1),
        ("https://rausgegangen.de/lubeck/kategorie/party/", 'Lübeck', 'Party', 1),
        ("https://rausgegangen.de/lubeck/kategorie/feste-und-festival/", 'Lübeck', 'Feste & Festival', 1),
        ("https://rausgegangen.de/flensburg/kategorie/konzerte-und-musik/", 'Flensburg', 'Konzerte & Musik', 1),
        ("https://rausgegangen.de/flensburg/kategorie/party/", 'Flensburg', 'Party', 1),
        ("https://rausgegangen.de/flensburg/kategorie/feste-und-festival/", 'Flensburg', 'Feste & Festival', 1)
    ]

    # Preparations for scraping (setting options, instantiating the Chrome driver, opening the website)
    options = Options()
    options.add_argument("--headless")  # Run Chromium in headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(), options=options)
    driver.get("https://rausgegangen.de/hamburg/kategorie/konzerte-und-musik/")

    # Closing the cookie window, if it appears
    try:
        wait = WebDriverWait(driver, 10)
        reject_button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'iubenda-cs-reject-btn')))
        reject_button.click()
    except Exception as e:
        print("No cookie window to reject, but no problem.")

    # Changing website language settings to German (if necessary, depends on used driver)
    try:
        sidemenu_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@aria-label="Sidemenu"]')))
        sidemenu_button.click()
        submit_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//form[@action="/i18n/setlang/"]//button[@type="submit"]')))
        submit_button.click() 
    except Exception as e:
        print("No language switch needed here.")

    time.sleep(4)

    # Creating a list to store events
    data = []

    # Iterating over the different urls to scrape and opening each of them
    for part in scraping:

        driver.get(part[0])
        time.sleep(4)

        # Navigating through the webpage by clicking on the "next" button as many times as heuristically defined for each url in the beginning
        for i in range(part[3]):

            # Finding all events per page, the specific web element representing an event is identified via class name
            # As some category and city combinations sometimes don't yield any results, a try-except statement is used, so that this doesn't make the overall process fail
            try:
                tiles = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'tile-medium')))

                # Iterating over all found event elements and extracting the required information
                # Some initial preprocessing is already applied to date and time information
                for tile in tiles:
                    source = tile.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    
                    date_time_element = tile.find_element(By.CLASS_NAME, 'text-sm')
                    date_time_text = date_time_element.text.split('|')
                    date = date_time_text[0].strip() if len(date_time_text) > 0 else ''
                    times = date_time_text[1].strip() if len(date_time_text) > 1 else ''

                    event_name = tile.find_element(By.CLASS_NAME, 'text-truncate--2').text

                    location = tile.find_element(By.CLASS_NAME, 'opacity-70').text

                    price = tile.find_element(By.CLASS_NAME, 'text-primary').text

                    # All information per event is stored into a dictionary and the dictionary is appended to the list of events
                    data.append({
                        "Source": source,
                        "Date": date,
                        "Time": times,
                        "Subject": event_name,
                        "Location": location,
                        "Price": price,
                        "City": part[1], # using the information specified explicitly related to each url
                        "Category": part[2], # using the information specified explicitly related to each url
                        "Music label": True # all scraped events from this website are music related
                    })

            except Exception as e:
                print(f"No events in this category and city combination")
                continue

            # Navigating by clicking on the "next" button, if possible and if more than one page of results is supposed to be scraped for the respective url
            if part[3] > 1:
                try:
                    next_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//li[@class="list-none"]/a[span[text()="Nächste"]]')))
                    next_button.click()
                    time.sleep(4)
                except Exception as e:
                    continue

    # Last steps: Creating the dataframe of raw data from the event list, closing the driver and returning the dataframe
    df_raw = pd.DataFrame(data)
    df_raw = df_raw[['Subject', 'Date', 'Time', 'Location', 'Price', 'Source', 'City', 'Category', 'Music label']]
    driver.close()

    return df_raw


# Preprocessing function

def preprocess_rausgegangen(df_raw):
    # Bringing the raw data into the agreed final data format (Changing column names into the agreed on final names, putting together description from source and ticket price, 
    # dropping not further needed columns, preprocessing date and converting final date format to YYYY-MM-DD (another helper function was added later because of change in agreed date format),
    # filling empties with " ", sorting the columns)
    df_raw.rename(columns={'Time': 'Start_time'}, inplace=True)
    df_raw["End_time"] = " "
    df_raw['Description'] = df_raw['Source'] + ' , Preis: ' + df_raw['Price']
    df_raw.drop(columns=['Source', 'Price'], inplace=True) 
    df_raw['Date'] = df_raw['Date'].apply(process_date)
    df_raw['Date'] = df_raw['Date'].apply(convert_date)
    df_raw.rename(columns={'Date': 'Start_date'}, inplace=True)
    df_raw['Start_date'] = df_raw['Start_date'].apply(convert_date_patch)
    df_raw["End_date"] = df_raw["Start_date"]
    df_raw.rename(columns={'Music label': 'Music_label'}, inplace=True)
    df_raw = df_raw.fillna(" ")
    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

# Mapping of German month abbreviations to correct numerical representations
month_mapping = {
    'Jan': '01',
    'Feb': '02',
    'Mär': '03',
    'Apr': '04',
    'Mai': '05',
    'Jun': '06',
    'Jul': '07',
    'Aug': '08',
    'Sep': '09',
    'Okt': '10',
    'Nov': '11',
    'Dez': '12'
}

def process_date(date_str):
    # Preprocessing date information if given in regular format
    if isinstance(date_str, str):
        parts = date_str.split(',')
        if len(parts) > 1:
            return parts[1].strip()
        else:
            return " "
    else:
        return " "

def convert_date(date_str):
    # Converting date format into DD.MM.
    day, month = date_str.split('. ')
    month_num = month_mapping[month]
    return f"{day}.{month_num}."

def convert_date_patch(date_str):
    # Function to convert date from DD.MM. to YYYY-MM-DD
    # Date format changed over the course of the project, so this function was added to comply with the new format
    if "." in date_str:
        current_date = datetime.now()
        day, month = map(int, date_str.strip('.').split('.'))
        next_year = current_date.year if (month > current_date.month or (month == current_date.month and day >= current_date.day)) else current_date.year + 1
        new_date = datetime(next_year, month, day)
        return new_date.strftime('%Y-%m-%d')
    else:
        return " "
    

# Recommended usage of the above functions

df_raw = scrape_rausgegangen_hh_ki_hl_fl()
df_prep = preprocess_rausgegangen(df_raw)
df_prep.to_csv("Scraped_Events_Rausgegangen_HH_KI_HL_FL.csv")
print(df_prep.head())
print(df_prep.info())