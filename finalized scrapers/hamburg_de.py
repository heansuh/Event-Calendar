# Script for Scraping Hamburg.de Event Calendar
# The functions scrape_hamburg_de() and preprocess_hamburg_de() can be used for the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import pandas as pd
import time


# Scraping function

def scrape_hamburg_de(days_in_advance=10): # Optional parameter for how many days in advance to scrape events for
    
    # Preparations for scraping (setting options, instantiating the Chrome driver, opening the website, defining waits)
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(), options=options)
    driver.get("https://www.hamburg.de/kultur/veranstaltungen")
    wait = WebDriverWait(driver, 10)

    try:
        # Open the embedded website from the iframe on the website that contains the filtering options and the calendar content
        driver.get('https://hamburgwhl.infomaxnet.de/veranstaltungen/?widgetToken=0kPi6WAFtDs.&amp;#-IMXEVENT-results')
        time.sleep(5)

        # Checking search without date in the filtering options (more stable than trying to insert a specific timeframe in the calendar view)
        ohne_datum_label = wait.until(EC.element_to_be_clickable((By.XPATH, "//label[@for='search_dateWithout_1']")))
        ohne_datum_label.click()
        time.sleep(3)
        submit_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@type='submit' and contains(text(), 'Jetzt Veranstaltungen suchen')]")))
        submit_button.click()

    except Exception as e: # If the option above fails, actively switch to iframe (but disadvantage of only showing limited events)
        try:
            iframe = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'iframe[src="https://hamburgwhl.infomaxnet.de/veranstaltungen/?widgetToken=0kPi6WAFtDs"]'))
            )
            driver.switch_to.frame(iframe)
        except Exception as e:
            print(f"An error occurred: {e}")

    # Defining how far to scroll down and load more events according to a heuristic to minimally cover the chosen number of days in advance 
    clicks = days_in_advance*30 + 100

    # Iterating over the number of clicks defined above and clicking on the "load more events" button that often (if possible)
    for i in range(clicks):
        time.sleep(2)
        try:
            button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, '-IMXEVENT-lazyLoadButton')))
            button.click()
        except StaleElementReferenceException:
            continue
        except TimeoutException:
            print("Button not found or not clickable within the timeout period.")
            break

    # Finding all loaded events on the whole page, the specific web element representing an event is identified via CSS selector
    articles = driver.find_elements(By.CSS_SELECTOR, 'article.-IMXEVNT-listElement')

    # Creating a list to store events
    events_data = []

    # Iterating over all found event elements and extracting the required information
    # As sometimes not all information is available per event, this part was made robust by using try-except statements
    # This part already also includes some preprocessing of the information into the right format
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

        # All information per event is stored into a dictionary and the dictionary is appended to the list of events
        events_data.append({
            'Category': event_category,
            'Title': event_title,
            'Date': date,
            'Time': time_x,
            'Location': location,
            'City': city,
            'Source': event_source
        })

    # Last steps: Creating the dataframe of raw data from the event list, closing the driver and returning the dataframe
    df = pd.DataFrame(events_data)
    driver.close()

    return df


# Preprocessing function

def preprocess_hamburg_de(df_raw):
    # Bringing the raw data into the agreed final data format (Changing column names into the agreed on final names, converting final date format from DD.MM.YYYY to YYYY-MM-DD, preprocessing start and end time,
    # dropping not further needed columns, checking the category information for music relatedness and adding music label True or False (with helper function), filling empties with " ", sorting the columns)
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
    # Helper function to process start time and, if stated, end time into correct format
    if '-' in time_str:
        start_time, end_time = time_str.split(' - ')
        end_time = end_time.replace(' Uhr', '')
    else:
        start_time = time_str.replace(' Uhr', '')
        end_time = ' '
    return pd.Series([start_time, end_time])

# List of strings contained in music related category titles
music = ['Konzert', 'konzert', 'Musik', 'musik', 'Party', 'party', 'Tanz', 'tanz', 'Festival', 'festival', 'Musical', 'musical', 'Jazz', 'jazz', 'Blues', 'blues', 'Country', 'country', 'Folk', 'folk', 'Rock', 'rock', 'Pop', 'pop', 'Klassik', 'klassik', 'Gospel', 'gospel', 'Chöre', 'chöre']

def check_music(category):
    # Function to check if an event is music related or not according to its category and provide the correct label
    for word in music:
        if word in category:
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

df_raw = scrape_hamburg_de(10)
df_prep = preprocess_hamburg_de(df_raw)
df_prep.to_csv("Scraped_Events_Hamburg.csv")
print(df_prep.head())
print(df_prep.info())