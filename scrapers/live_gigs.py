# Script for Live Gigs (Hamburg, Schleswig Holstein) Event Calendar
# The functions scrape_live_gigs_hh_sh() and preprocess_live_gigs() should be imported to the script defining the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
import time


# Scraping function

def scrape_live_gigs_hh_sh():

    url = "https://www.livegigs.de/neumuenster/umkreis-100#Termine"
    driver = webdriver.Firefox()
    driver.get(url)
    wait = WebDriverWait(driver, 10)

    # cookie rejection
    try:
        button = wait.until(EC.element_to_be_clickable((By.ID, 'cookie-accept-required')))
        button.click()
    except Exception as e:
        print(f"An error occurred: {e}")

    # Initialize a list to store the extracted information
    events_data = []

    for i in range(2):

        # Find all elements with the class 'box-eventline'
        elements = driver.find_elements(By.CLASS_NAME, 'box-eventline')

        # Iterate over each element and extract the required information
        for element in elements:
            try:
                title = element.find_element(By.CLASS_NAME, 'summary').get_attribute('title')
                title = title.split(" - ")[0]
            except:
                #title = None
                continue

            try:
                source = element.find_element(By.CLASS_NAME, 'summary').get_attribute('href')
            except:
                source = None

            try:
                time_standard = element.find_element(By.CLASS_NAME, 'time').text[:6].strip()
            except:
                time_standard = None
            
            try:
                # Extract day, month, and year
                day = element.find_element(By.CLASS_NAME, 'day').text
                month = element.find_element(By.CLASS_NAME, 'month').get_attribute('title').split('-')[1]
                year = element.find_element(By.CLASS_NAME, 'year').text
                # Format date as DD.MM.YYYY
                formatted_date = f"{day}.{month}.{year}"
            except:
                formatted_date = None

            try:
                category = element.find_element(By.CLASS_NAME, 'category').text
            except:
                category = None

            try:
                location = element.find_element(By.CLASS_NAME, 'venue').get_attribute('title')
            except:
                location = None

            try:
                city = element.find_element(By.CLASS_NAME, 'city').text
            except:
                city = None

            # Append the extracted information as a dictionary to the list
            events_data.append({
                'Subject': title,
                'Description': source,
                'Start_time': time_standard,
                'End_time': "N/A",
                'Start_date': formatted_date, #label_date,
                'End_date': formatted_date,
                'Category': category,
                'Location': location,
                'City': city,
                'Music_label': "music"
            })

        # nur einmal weiterblättern, dann sind schon die events des nächsten monats (+puffer) abgedeckt
        if i < 1:
            try:
                next_day_link = driver.find_element(By.XPATH, '//div[@class="standard link-text"]/a[contains(text(), "nächster Tag")]')
                next_day_link.click()
                time.sleep(5)
            except Exception as e:
                print(f"An error occurred: {e}")
                break

    df_raw = pd.DataFrame(events_data)
    driver.close()
    return df_raw


# Preprocessing function

def preprocess_live_gigs(df_raw):
    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Example usage

df_raw = scrape_live_gigs_hh_sh()
df_prep = preprocess_live_gigs(df_raw)
df_prep.to_csv("Scraped_Events_Live_Gigs_HH_SH.csv")