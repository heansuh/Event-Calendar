# Script for Rausgegangen.de (Hamburg, Kiel, Lübeck, Flensburg) Event Calendar
# The functions scrape_rausgegangen_hh_ki_hl_fl() and preprocess_rausgegangen() should be imported to the script defining the main scraping process

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

    # urls and relevant info to scrape
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

    # preparations (cookie rejection, language settings etc)
    options = Options()
    options.add_argument("--headless")  # Run Chromium in headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(), options=options)
    #driver = webdriver.Firefox()
    driver.get("https://rausgegangen.de/hamburg/kategorie/konzerte-und-musik/")

    # cookie rejection
    try:
        wait = WebDriverWait(driver, 10)
        reject_button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'iubenda-cs-reject-btn')))
        reject_button.click()
    except Exception as e:
        print(f"An error occurred: {e}")

    # change website to German (TODO unter chrome nicht nötig, aber stört den Prozess auch nicht)
    try:
        sidemenu_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@aria-label="Sidemenu"]')))
        sidemenu_button.click()
        submit_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//form[@action="/i18n/setlang/"]//button[@type="submit"]')))
        submit_button.click()
        
    except Exception as e:
        print(f"An error occurred: {e}")

    time.sleep(4)
    #actual scraping process

    data = []

    for part in scraping:

        driver.get(part[0])
        time.sleep(4)

        #extract events
        for i in range(part[3]):
            try:
                tiles = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'tile-medium')))

                for tile in tiles:
                    source = tile.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    
                    # Extract the date and time
                    date_time_element = tile.find_element(By.CLASS_NAME, 'text-sm')
                    date_time_text = date_time_element.text.split('|')
                    date = date_time_text[0].strip() if len(date_time_text) > 0 else ''
                    times = date_time_text[1].strip() if len(date_time_text) > 1 else ''

                    # Extract the event name
                    event_name = tile.find_element(By.CLASS_NAME, 'text-truncate--2').text

                    # Extract the location
                    location = tile.find_element(By.CLASS_NAME, 'opacity-70').text

                    # Extract the price
                    price = tile.find_element(By.CLASS_NAME, 'text-primary').text

                    # Append the extracted information to the list
                    data.append({
                        "Source": source,
                        "Date": date,
                        "Time": times,
                        "Subject": event_name,
                        "Location": location,
                        "Price": price,
                        "City": part[1],
                        "Category": part[2],
                        "Music label": True
                    })

            except Exception as e:
                print(f"No events in this category and city combination")
                continue

            if part[3] > 1:
                try:
                    next_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//li[@class="list-none"]/a[span[text()="Nächste"]]')))
                    next_button.click()
                    time.sleep(4)

                except Exception as e:
                    continue

    df_raw = pd.DataFrame(data)
    df_raw = df_raw[['Subject', 'Date', 'Time', 'Location', 'Price', 'Source', 'City', 'Category', 'Music label']]

    driver.close()
    
    return df_raw


# Preprocessing function

def preprocess_rausgegangen(df_raw):

    df_raw.rename(columns={'Time': 'Start_time'}, inplace=True)
    df_raw["End_time"] = " "

    df_raw['Description'] = df_raw['Source'] + ' , Preis: ' + df_raw['Price']
    df_raw.drop(columns=['Source', 'Price'], inplace=True)

    df_raw['Date'] = df_raw['Date'].str.split(',').str[1].str.strip()
    df_raw['Date'] = df_raw['Date'].apply(convert_date)
    df_raw.rename(columns={'Date': 'Start_date'}, inplace=True)
    df_raw['Start_date'] = df_raw['Start_date'].apply(convert_date_patch)
    df_raw["End_date"] = df_raw["Start_date"]

    df_raw.rename(columns={'Music label': 'Music_label'}, inplace=True)

    df_raw = df_raw.fillna(" ")

    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

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

def convert_date(date_str):
    day, month = date_str.split('. ')
    month_num = month_mapping[month]
    return f"{day}.{month_num}."

# Function to convert date from DD.MM. to YYYY-MM-DD
def convert_date_patch(date_str):
    
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

df_raw = scrape_rausgegangen_hh_ki_hl_fl()
df_prep = preprocess_rausgegangen(df_raw)
df_prep.to_csv("Scraped_Events_Rausgegangen_HH_KI_HL_FL.csv")
print(df_prep.head())
print(df_prep.info())