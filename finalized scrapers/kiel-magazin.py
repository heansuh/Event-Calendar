# Script for Kiel-Magazin (Kiel, Schleswig Holstein) Event Calendar
# The functions scrape_kiel_magazin() and preprocess_kiel_magazin() should be imported to the script defining the main scraping process

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

def scrape_kiel_magazin(days_in_advance=30):

    days_in_advance = days_in_advance
    today = datetime.today().date()
    today_plus_x = today + timedelta(days=days_in_advance)
    i = 1

    # preparations
    options = Options()
    options.add_argument("--headless")  # Run Chromium in headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    url = f"https://www.kiel-magazin.de/veranstaltungssuche/konzerte/0/{today.year}-{today.month:02d}-{today.day:02d}/{today_plus_x.year}-{today_plus_x.month:02d}-{today_plus_x.day:02d}/0/{i}"

    #driver = webdriver.Firefox()
    driver = webdriver.Chrome(service=Service(), options=options)
    driver.get(url)
    time.sleep(5)
    wait = WebDriverWait(driver, 10)
    events = []

    #cookie rejection
    try:
        button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.button.ccm--save-settings.ccm--button-primary.ccm--ctrl-init[data-full-consent="true"]')))
        button.click()
    except Exception as e:
        print(f"An error occurred: {e}")

    try:
        h1_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.color-blue.section__hl.event__search--hl'))
        )
        x = int(h1_element.text.split("/")[-1])
    except Exception as e:
        print("Page element not found")
        x = 5

    for i in range(1,x+1):

        url = f"https://www.kiel-magazin.de/veranstaltungssuche/konzerte/0/{today.year}-{today.month:02d}-{today.day:02d}/{today_plus_x.year}-{today_plus_x.month:02d}-{today_plus_x.day:02d}/0/{i}"
        driver.get(url)
        time.sleep(5)

        articles = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'article.card.card__event')))

        for article in articles:
            try:
                title_element = article.find_element(By.CSS_SELECTOR, 'a.card-link')
                title = title_element.get_attribute('title')[19:-1]
                source_url = title_element.get_attribute('href')

                date_and_location_element = article.find_element(By.CSS_SELECTOR, 'p.card-date')
                date_and_location = date_and_location_element.get_attribute('innerHTML').split('<br>')

                date = date_and_location[0].strip()
                date = date.split(',')[1]
                if "ab" in date:
                    datedate = date.split(" ab ")[0]
                    times = date.split(" ab ")[1][:-4]
                else:
                    datedate = date
                    times = " "

                location = date_and_location[1].strip() if len(date_and_location) > 1 else ""

                category_element = article.find_element(By.CSS_SELECTOR, 'p.card-category')
                category = category_element.text.strip()

                event = {
                    "Subject": title,
                    "Description": source_url,
                    "Start_date": datedate,
                    "End_date": datedate,
                    "Start_time": times,
                    "End_time": " ",
                    "Location": location.split(',')[0],
                    "City": location.split(',')[-1],
                    "Category": category,
                    "Music_label": True
                }
                events.append(event)
            except Exception as e:
                print(f"An error occurred while processing an article: {e}")
                continue

    df_raw = pd.DataFrame(events)
    driver.close()

    return df_raw


# Preprocessing function

def preprocess_kiel_magazin(df_raw):

    df_raw['Start_date'] = df_raw['Start_date'].str.strip().apply(add_leading_zero_to_day).apply(convert_date_to_yyyy_mm_dd)
    df_raw['End_date'] = df_raw['End_date'].str.strip().apply(add_leading_zero_to_day).apply(convert_date_to_yyyy_mm_dd)
    
    #df_raw['Start_date'] = df_raw['Start_date'].str.strip()
    #df_raw['End_date'] = df_raw['End_date'].str.strip()
    #df_raw['Start_date'] = pd.to_datetime(df_raw['Start_date'], format='%d. %B %Y', dayfirst=True).dt.strftime('%Y-%m-%d')
    #df_raw['End_date'] = pd.to_datetime(df_raw['End_date'], format='%d. %B %Y', dayfirst=True).dt.strftime('%Y-%m-%d')
    
    df_raw[['Start_time', 'End_time']] = df_raw['Start_time'].str.split(' bis ', expand=True)  

    df_raw['City'] = df_raw['City'].str.title()
    df_raw = df_raw.fillna(" ")

    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

def add_leading_zero_to_day(date_str):
    parts = date_str.split('. ')
    if len(parts[0]) == 1:
        parts[0] = '0' + parts[0]
    return ' '.join(parts)

def convert_date_to_yyyy_mm_dd(date_str):
    parts = date_str.split(' ')
    day = parts[0]
    month = month_mapping[parts[1]]
    year = parts[2]
    return f"{year}-{month}-{day}"

month_mapping = {
    'Januar': '01',
    'Februar': '02',
    'März': '03',
    'April': '04',
    'Mai': '05',
    'Juni': '06',
    'Juli': '07',
    'August': '08',
    'September': '09',
    'Oktober': '10',
    'November': '11',
    'Dezember': '12'
}


# Example usage

df_raw = scrape_kiel_magazin(30)
#df_raw.to_csv("Test1234.csv")
df_prep = preprocess_kiel_magazin(df_raw)
df_prep.to_csv("Scraped_Events_Kiel_Magazin.csv")
print(df_prep.head())
print(df_prep.info())