# Script for Scraping SH-Tourismus Event Calendar
# The functions scrape_sh_tourismus() and preprocess_sh_tourismus() should be imported to the script defining the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException

import pandas as pd


# Scraping function

def scrape_sh_tourismus(days_in_advance=10):
    
    driver = webdriver.Firefox()
    driver.get('https://www.sh-tourismus.de/veranstaltungskalender')

    iframe = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'iframe[src*="tashwhl.infomaxnet.de"]'))
    )
    driver.switch_to.frame(iframe)

    wait = WebDriverWait(driver, 10)
    clicks = days_in_advance*30 + 100 

    for i in range(clicks): #TODO maybe add sleeps
        try:
            button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, '-IMXEVENT-lazyLoadButton')))
            button.click()
        except StaleElementReferenceException:
            continue
        except TimeoutException:
            print("Button not found or not clickable within the timeout period.")
            break

    articles = driver.find_elements(By.CSS_SELECTOR, 'article.-IMXEVNT-listElement')


    events_data = []

    for article in articles:
        try:
            event_category = article.find_element(By.CSS_SELECTOR, 'p.-IMXEVNT-listElement__text__subline').text
        except NoSuchElementException:
            event_category = ' '

        try:
            event_title = article.find_element(By.CSS_SELECTOR, 'span.-IMXEVNT-title').get_attribute('data-uppertitle')
        except NoSuchElementException:
            event_title = ' '

        try:
            event_date_location = article.find_element(By.CSS_SELECTOR, 'p.-IMXEVNT-listElement__text__info').text
        except NoSuchElementException:
            event_date_location = ' '

        if event_date_location != ' ':
            parts = [x.strip() for x in event_date_location.split('/')]
        else:
            parts = []

        if len(parts) >= 3:
            date, time, location = parts[0], parts[1], ' / '.join(parts[2:])
        elif len(parts) == 2:
            date, time, location = parts[0], parts[1], ''
        else:
            date, time, location = ' ', ' ', ' '

        if ',' in location:
            location, city = [x.strip() for x in location.rsplit(',', 1)]
        else:
            location, city = location, ' '

        try:
            event_source = article.find_element(By.CSS_SELECTOR, 'h2 a').get_attribute('href')
        except NoSuchElementException:
            event_title = ' '

        events_data.append({
            'Category': event_category,
            'Title': event_title,
            'Date': date,
            'Time': time,
            'Location': location,
            'City': city,
            'Source': event_source
        })

    df = pd.DataFrame(events_data)

    driver.close()

    return df


# Preprocessing function

def preprocess_sh_tourismus(df_raw):

    df_raw.rename(columns={'Title': 'Subject'}, inplace=True)

    df_raw.rename(columns={'Date': 'Start_date'}, inplace=True)
    df_raw["End_date"] = df_raw["Start_date"]

    df_raw[['Start_time', 'End_time']] = df_raw['Time'].apply(preprocess_time)
    df_raw.drop(columns=['Time'], inplace=True)

    df_raw.rename(columns={'Source': 'Description'}, inplace=True)    

    df_raw['Music_label'] = df_raw['Category'].apply(check_music)

    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

def preprocess_time(time_str):
    if '-' in time_str:
        start_time, end_time = time_str.split(' - ')
        end_time = end_time.replace(' Uhr', '')
    else:
        start_time = time_str.replace(' Uhr', '')
        end_time = ' '
    return pd.Series([start_time, end_time])


music = ['Konzert', 'konzert', 'Musik', 'musik', 'Party', 'party', 'Tanz', 'tanz', 'Festival', 'festival', 'Musical', 'musical', 'Jazz', 'jazz', 'Blues', 'blues', 'Country', 'country', 'Folk', 'folk', 'Rock', 'rock', 'Pop', 'pop', 'Klassik', 'klassik', 'Gospel', 'gospel', 'Chöre', 'chöre']

def check_music(category):
    for word in music:
        if word in category:
            return True
    return False


# Example usage

df_raw = scrape_sh_tourismus(10)
df_prep = preprocess_sh_tourismus(df_raw)
df_prep.to_csv("Scraped_Events_Schleswig_Holstein.csv")

