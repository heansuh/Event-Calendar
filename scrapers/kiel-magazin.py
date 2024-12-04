# Script for Kiel-Magazin (Kiel, Schleswig Holstein) Event Calendar
# The functions scrape_kiel_magazin() and preprocess_kiel_magazin() should be imported to the script defining the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
import time
from datetime import datetime, timedelta


# Scraping function

def scrape_kiel_magazin(days_in_advance=30):

    days_in_advance = 30
    today = datetime.today().date()
    today_plus_x = today + timedelta(days=days_in_advance)
    i = 1

    url = f"https://www.kiel-magazin.de/veranstaltungssuche/konzerte/0/{today.year}-{today.month:02d}-{today.day:02d}/{today_plus_x.year}-{today_plus_x.month:02d}-{today_plus_x.day:02d}/0/{i}"

    driver = webdriver.Firefox()
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

    df_raw['Start_date'] = df_raw['Start_date'].str.strip()
    df_raw['End_date'] = df_raw['End_date'].str.strip()
    df_raw['Start_date'] = pd.to_datetime(df_raw['Start_date'], format='%d. %B %Y', dayfirst=True).dt.strftime('%Y-%m-%d')
    df_raw['End_date'] = pd.to_datetime(df_raw['End_date'], format='%d. %B %Y', dayfirst=True).dt.strftime('%Y-%m-%d')
    
    df_raw[['Start_time', 'End_time']] = df_raw['Start_time'].str.split(' bis ', expand=True)  

    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Example usage

df_raw = scrape_kiel_magazin(30)
df_prep = preprocess_kiel_magazin(df_raw)
df_prep.to_csv("Scraped_Events_Kiel_Magazin.csv")