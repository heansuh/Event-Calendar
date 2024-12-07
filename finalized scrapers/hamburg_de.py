# Script for Scraping Hamburg.de Event Calendar
# The functions scrape_hamburg_de() and preprocess_hamburg_de() should be imported to the script defining the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException

import pandas as pd
import time


# Scraping function

def scrape_hamburg_de(days_in_advance=10):
    
    driver = webdriver.Firefox()
    driver.get("https://www.hamburg.de/kultur/veranstaltungen")
    wait = WebDriverWait(driver, 10)

    #wechsel von heute auf alle daten (spez auswahl zu errorprone lieber heuristik aber stabil)

    try:
        # Open the webpage
        driver.get('https://hamburgwhl.infomaxnet.de/veranstaltungen/?widgetToken=0kPi6WAFtDs.&amp;#-IMXEVENT-results')

        time.sleep(5)

        # Wait until the "Ohne Datum suchen" label is present and clickable
        ohne_datum_label = wait.until(EC.element_to_be_clickable((By.XPATH, "//label[@for='search_dateWithout_1']")))

        # Click the "Ohne Datum suchen" label
        ohne_datum_label.click()

        time.sleep(3)

        # Wait until the submit button is present and clickable
        submit_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@type='submit' and contains(text(), 'Jetzt Veranstaltungen suchen')]")))

        # Click the submit button
        submit_button.click()

    except Exception as e: #dann kann man leider nur für heute suchen aber immerhin
        try:
            # Switch to the iframe
            iframe = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'iframe[src="https://hamburgwhl.infomaxnet.de/veranstaltungen/?widgetToken=0kPi6WAFtDs"]'))
            )
            driver.switch_to.frame(iframe)
        except Exception as e:
            print(f"An error occurred: {e}")

    clicks = days_in_advance*30 + 100 #puffer kann deutlich reduziert evtl sogar gelöscht werden TODO an HH anpassen!

    for i in range(clicks): #TODO evaluate sleeps
        time.sleep(2)
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
            date, time_x, location = parts[0], parts[1], ' / '.join(parts[2:]) 
        elif len(parts) == 2:
            date, time_x, location = parts[0], parts[1], ''
        else:
            date, time_x, location = ' ', ' ', ' '

        if ',' in location:
            location, city = [x.strip() for x in location.rsplit(',', 1)]
        else:
            location, city = location, ' '

        try:
            event_source = article.find_element(By.CSS_SELECTOR, 'h2 a').get_attribute('href')
        except NoSuchElementException:
            event_source = ' '

        events_data.append({
            'Category': event_category,
            'Title': event_title,
            'Date': date,
            'Time': time_x,
            'Location': location,
            'City': city,
            'Source': event_source
        })

    df = pd.DataFrame(events_data)

    driver.close()

    return df


# Preprocessing function

def preprocess_hamburg_de(df_raw):

    df_raw.rename(columns={'Title': 'Subject'}, inplace=True)

    df_raw.rename(columns={'Date': 'Start_date'}, inplace=True)
    df_raw["Start_date"] = df_raw["Start_date"].apply(convert_date_format)
    df_raw["End_date"] = df_raw["Start_date"]

    df_raw[['Start_time', 'End_time']] = df_raw['Time'].apply(preprocess_time)
    df_raw.drop(columns=['Time'], inplace=True)

    df_raw.rename(columns={'Source': 'Description'}, inplace=True)    

    df_raw['Music_label'] = df_raw['Category'].apply(check_music)

    df_raw = df_raw.fillna(" ")

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

def convert_date_format(date_str):
    date_str = str(date_str)
    if "." in date_str:
        return pd.to_datetime(date_str, format='%d.%m.%Y').strftime('%Y-%m-%d')
    else:
        return " "
    

# Example usage

df_raw = scrape_hamburg_de(10)
df_prep = preprocess_hamburg_de(df_raw)
df_prep.to_csv("Scraped_Events_Hamburg.csv")