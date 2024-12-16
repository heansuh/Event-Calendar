# Script for Scraping Kiel-Sailing-City Event Calendar
# The functions scrape_kiel_sailing_city() and preprocess_kiel_sailing_city() can be used for the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import pandas as pd
from datetime import datetime, timedelta
import time


# Scraping function

def scrape_kiel_sailing_city(days_in_advance=10): # Optional parameter for how many days in advance to scrape events for
    
    # Preparations for scraping (setting options, instantiating the Chrome driver, opening the website)
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(), options=options)
    driver.get('https://kiel-sailing-city.de/veranstaltungen/kalender')
    time.sleep(5)

    # Closing the cookie window
    try:
        decline_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.cm-btn.cm-btn-danger.cn-decline"))
        )
        decline_button.click()
    except Exception as e:
        print(f"An error occurred: {e}")

    # Generating a string of the timeframe to scrape events for (with helper function)
    new_date_string = generate_new_day_string(days_in_advance=days_in_advance)
    print(new_date_string)
    time.sleep(2)

    # Entering the defined timeframe into the filtering input field on the website (showed as stable)
    try:
        input_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[placeholder='DD.MM.YYYY  -  DD.MM.YYYY']"))
        )
        input_field.click()
        input_field.clear()
        input_field.send_keys(new_date_string)
    except Exception as e:
        print(f"An error occurred: {e}")

    time.sleep(2)

    # Creating a list to store events
    all_data = []

    # Navigation: Scrolling down all the website until it is not further possible to load all events on the website (orientation through scroll position)
    previous_scroll_position = 0
    try:
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            current_scroll_position = driver.execute_script("return window.pageYOffset;")

            # Once it is not possible to scroll down further, retrieving all event information (with helper function) and exiting the loop
            if current_scroll_position == previous_scroll_position:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                all_data = extract_elements(driver=driver)
                break
            previous_scroll_position = current_scroll_position    
    except Exception as e:
        print(f"An error occurred: {e}")

    # Splitting up date and time details and replacing the string "today" with the current date (with helper function)
    current_date = datetime.now().strftime('%d.%m.%Y')
    for item in all_data:
        date, time_clock = parse_time_details(item['Time Details'], current_date=current_date)
        item['Date'] = date
        item['Time'] = time_clock
    
    # Last steps: Creating the dataframe of raw data from the event list, dropping the already preprocessed Time Details column, closing the driver and returning the dataframe
    df = pd.DataFrame(all_data)
    df = df.drop(columns=['Time Details'])
    driver.close()

    return df


# Preprocessing function

def preprocess_kiel_sailing_city(df_raw):
    # Bringing the raw data into the agreed final data format (Changing column names into the agreed on final names, converting date format to YYYY-MM-DD, 
    # processing time details into start time and if given end time, checking the category information for music relatedness and adding music label True or False (with helper function),
    # setting city as Kiel for this scraper, filling empties with " ", sorting the columns)
    df_raw.rename(columns={'Title': 'Subject'}, inplace=True)
    df_raw.rename(columns={'Date': 'Start_date'}, inplace=True)
    df_raw["Start_date"] = df_raw["Start_date"].apply(convert_date_format)
    df_raw["End_date"] = df_raw["Start_date"]
    df_raw[['Start_time', 'End_time']] = df_raw['Time'].apply(preprocess_time)
    df_raw.drop(columns=['Time'], inplace=True)
    df_raw.rename(columns={'Source': 'Description'}, inplace=True)    
    df_raw['Music_label'] = df_raw['Categories'].apply(check_music)
    df_raw.rename(columns={'Categories': 'Category'}, inplace=True) 
    df_raw["City"] = "Kiel"
    df_raw = df_raw.fillna(" ")
    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

def generate_new_day_string(days_in_advance=10):
    # Function to generate a date string to input as timeframe on the website in DD.MM.YYYY - DD.MM.YYYY format
    current_date = datetime.now()
    today = current_date.strftime("%d.%m.%Y")
    future_date = current_date + timedelta(days=days_in_advance)
    formatted_future_date = future_date.strftime("%d.%m.%Y")
    new_date_string = today + " - " + formatted_future_date
    return new_date_string


def extract_elements(driver):

    # Finding all loaded events on the whole page, the specific web element representing an event is identified via CSS selector
    elements = driver.find_elements(By.CSS_SELECTOR, "li.flex.flex-nowrap.bg-gray-200.min-h-\\[430px\\].flex-col")

    # Creating a list to store events
    extracted_data = []
    
    # Iterating over all found event elements and extracting the required information
    # To make this process robust a try-except statement is used, so that missing information an one event doesn't lead to a failure of the whole process but just to leaving out that event
    for element in elements:
        try:
            title = element.find_element(By.CSS_SELECTOR, "h4.c-headline.c-rich-text.truncate").text
            
            location = element.find_element(By.CSS_SELECTOR, "span.text-base p").text
            
            categories = [span.text for span in element.find_elements(By.CSS_SELECTOR, "div.my-3.flex.flex-wrap span")]
            
            time_details = element.find_element(By.CSS_SELECTOR, "div.text-xs").text

            source_link = element.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
            
            # All information per event is stored into a dictionary and the dictionary is appended to the list of events
            extracted_data.append({
                "Title": title,
                "Location": location,
                "Categories": categories,
                "Time Details": time_details,
                "Source": source_link
            })
        except Exception as e:
            print(f"An error occurred while extracting element data: {e}")
    
    # Handing back the list of dictionaries of events to the scraping function defined above
    return extracted_data


def parse_time_details(time_details, current_date):
    # Function to seperate date and time and to replace "today" with the current date
    date_time = time_details.split('\n')[0]
    if 'Heute' in date_time:
        date_time = date_time.replace('Heute', current_date)
    date, time_clock = date_time.split(' ', 1)
    return date, time_clock


def check_music(category_list):
    # Function to check if an event is music related or not according to its category and provide the correct label
    for category in category_list:
        for word in music:
            if word in category:
                return True
    return False

def preprocess_time(time_str):
    # Function to preprocess time details from the format given on the website to the agreed final data format of the project
    if '-' in time_str:
        start_time, end_time = time_str.split(' - ')
        end_time = end_time.replace(' Uhr', '')
    else:
        start_time = time_str.replace(' Uhr', '')
        end_time = ' '
    return pd.Series([start_time, end_time])

def convert_date_format(date_str):
    # Function for converting date format from DD.MM.YYYY to YYYY-MM-DD 
    # Date format changed over the course of the project, so this function was added to older scrapers
    date_str = str(date_str)
    if "." in date_str:
        return pd.to_datetime(date_str, format='%d.%m.%Y').strftime('%Y-%m-%d')
    else:
        return " "

# List of strings contained in music related category titles
music = ['Nachtleben', 'Konzert', 'konzert', 'Musik', 'musik', 'Party', 'party', 'Tanz', 'tanz', 'Festival', 'festival', 'Musical', 'musical', 'Jazz', 'jazz', 'Blues', 'blues', 'Country', 'country', 'Folk', 'folk', 'Rock', 'rock', 'Pop', 'pop', 'Klassik', 'klassik', 'Gospel', 'gospel', 'Chöre', 'chöre']


# Recommended usage of the above functions

df_raw = scrape_kiel_sailing_city(10)
df_prep = preprocess_kiel_sailing_city(df_raw)
df_prep.to_csv("Scraped_Events_Kiel_Sailing_City.csv")
print(df_prep.head())
print(df_prep.info())