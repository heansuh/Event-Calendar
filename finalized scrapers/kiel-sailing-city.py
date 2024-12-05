# Script for Scraping Kiel-Sailing-City Event Calendar
# The functions scrape_kiel_sailing_city() and preprocess_kiel_sailing_city() should be imported to the script defining the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
from datetime import datetime, timedelta
import time


# Scraping function

def scrape_kiel_sailing_city(days_in_advance=10):
    
    driver = webdriver.Firefox()
    driver.get('https://kiel-sailing-city.de/veranstaltungen/kalender')
    time.sleep(5)

    # Cookie rejection
    try:
        decline_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.cm-btn.cm-btn-danger.cn-decline"))
        )
        decline_button.click()
    except Exception as e:
        print(f"An error occurred: {e}")

    new_date_string = generate_new_day_string(days_in_advance=days_in_advance)
    print(new_date_string)
    time.sleep(2)

    # Zeitraum auswählen zb 10 tage im voraus
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
    previous_scroll_position = 0
    all_data = []

    # Bis ans Ende scrollen
    try:
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            current_scroll_position = driver.execute_script("return window.pageYOffset;")
            if current_scroll_position == previous_scroll_position:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                all_data = extract_elements(driver=driver)
                break
            previous_scroll_position = current_scroll_position
            
    except Exception as e:
        print(f"An error occurred: {e}")

    # Current date
    current_date = datetime.now().strftime('%d.%m.%Y')

    # Parse the "Time Details" and create new columns
    for item in all_data:
        date, time_clock = parse_time_details(item['Time Details'], current_date=current_date)
        item['Date'] = date
        item['Time'] = time_clock
    
    df = pd.DataFrame(all_data)
    df = df.drop(columns=['Time Details'])

    driver.close()

    return df


# Preprocessing function

def preprocess_kiel_sailing_city(df_raw):

    df_raw.rename(columns={'Title': 'Subject'}, inplace=True)

    df_raw.rename(columns={'Date': 'Start_date'}, inplace=True)
    df_raw["Start_date"] = df_raw["Start_date"].apply(convert_date_format)
    #df_raw["Start_date"] = pd.to_datetime(df_raw["Start_date"], format='%d.%m.%Y').dt.strftime('%Y-%m-%d') #same as eventim TODO
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
    # Get the current date
    current_date = datetime.now()
    today = current_date.strftime("%d.%m.%Y")

    # Calculate the date 10 days from today
    future_date = current_date + timedelta(days=days_in_advance)
    # Format the future date as DD.MM.YYYY
    formatted_future_date = future_date.strftime("%d.%m.%Y")

    new_date_string = today + " - " + formatted_future_date
    return new_date_string


def extract_elements(driver):
    elements = driver.find_elements(By.CSS_SELECTOR, "li.flex.flex-nowrap.bg-gray-200.min-h-\\[430px\\].flex-col")
    extracted_data = []
    
    for element in elements:
        try:
            # Extract the title
            title = element.find_element(By.CSS_SELECTOR, "h4.c-headline.c-rich-text.truncate").text
            
            # Extract the location
            location = element.find_element(By.CSS_SELECTOR, "span.text-base p").text
            
            # Extract the category tags
            categories = [span.text for span in element.find_elements(By.CSS_SELECTOR, "div.my-3.flex.flex-wrap span")]
            
            # Extract the time details
            time_details = element.find_element(By.CSS_SELECTOR, "div.text-xs").text

            # Extract source link
            source_link = element.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
            
            # Store the extracted information
            extracted_data.append({
                "Title": title,
                "Location": location,
                "Categories": categories,
                "Time Details": time_details,
                "Source": source_link
            })
        except Exception as e:
            print(f"An error occurred while extracting element data: {e}")
    
    return extracted_data


# Function to parse "Time Details"
def parse_time_details(time_details, current_date):
    date_time = time_details.split('\n')[0]
    if 'Heute' in date_time:
        date_time = date_time.replace('Heute', current_date)
    date, time_clock = date_time.split(' ', 1)
    return date, time_clock


def check_music(category_list):
    for category in category_list:
        for word in music:
            if word in category:
                return True
    return False

def preprocess_time(time_str):
    if '-' in time_str:
        start_time, end_time = time_str.split(' - ')
        end_time = end_time.replace(' Uhr', '')
    else:
        start_time = time_str.replace(' Uhr', '')
        end_time = ' '
    return pd.Series([start_time, end_time])

def convert_date_format(date_str):
    date_str = str(date_str)
    if "." in date_str:
        return pd.to_datetime(date_str, format='%d.%m.%Y').strftime('%Y-%m-%d')
    else:
        return " "

music = ['Nachtleben', 'Konzert', 'konzert', 'Musik', 'musik', 'Party', 'party', 'Tanz', 'tanz', 'Festival', 'festival', 'Musical', 'musical', 'Jazz', 'jazz', 'Blues', 'blues', 'Country', 'country', 'Folk', 'folk', 'Rock', 'rock', 'Pop', 'pop', 'Klassik', 'klassik', 'Gospel', 'gospel', 'Chöre', 'chöre']


# Example usage

df_raw = scrape_kiel_sailing_city(10)
df_prep = preprocess_kiel_sailing_city(df_raw)
df_prep.to_csv("Scraped_Events_Kiel_Sailing_City.csv")