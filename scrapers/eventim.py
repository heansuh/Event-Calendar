# Script for Eventim (Hamburg, Schleswig Holstein) Event Calendar
# The functions scrape_eventim() and preprocess_eventim() should be imported to the script defining the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
import time
from datetime import datetime, timedelta


# Scraping function

def scrape_eventim(days_in_advance=30):

    # Get the current date
    today = datetime.today().date()
    # Get the date x days from today
    today_plus_x = today + timedelta(days=days_in_advance)

    #define url
    url = f"https://www.eventim.de/events/konzerte-1/?zipcode=24534&distance=100&shownonbookable=true&sort=DateAsc&dateFrom={today.year}-{today.month}-{today.day}&dateTo={today_plus_x.year}-{today_plus_x.month}-{today_plus_x.day}"

    #prepare scraping
    driver = webdriver.Firefox()
    driver.get(url)
    time.sleep(5)
    wait = WebDriverWait(driver, 10)

    # cookie rejection
    try:
        element = driver.find_element(By.ID, "cmpwelcomebtnno")
        element.click()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    extracted_info = [] 

    while True:
        elements = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "product-group-item")))

        # Iterate over each element and extract the required information
        for element in elements:
            try:
                # Extract the title
                title_element = element.find_element(By.CSS_SELECTOR, '[id^="listing-headline"]')#(By.ID, "listing-headline-0")
                title = title_element.text

                # Extract the location/date/time
                location_date_time_element = element.find_element(By.CSS_SELECTOR, ".text-overflow-ellipsis.u-text-color.theme-text-color")
                location_date_time = location_date_time_element.text

                # Extract the description (if it exists)
                try:
                    description_element = element.find_element(By.CSS_SELECTOR, ".listing-description.theme-text-color.text-overflow-ellipsis.hidden-xs")
                    description = description_element.text
                except Exception as e:
                    description = None

                # Extract the source (href)
                source_element = element.find_element(By.CSS_SELECTOR, "a.btn.btn-sm.btn-block.btn-primary")
                source = source_element.get_attribute("href")

                # Append the extracted information to the list
                extracted_info.append({
                    "Subject": title,
                    "Location_Date_Time": location_date_time,
                    "Description": description,
                    "Source": source,
                    "Category": "Konzert",
                    "Music_label": "music"
                })

            except Exception as e:
                continue
        
        try:
            pagination_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "pagination-item a[data-qa='nextPage']")))
            pagination_element.click()
            time.sleep(5)
        except Exception as e:
            print("No more pages")
            break

    df_raw = pd.DataFrame(extracted_info)
    driver.close()
    return df_raw


# Preprocessing function

def preprocess_eventim(df_raw):

    df_raw[['Location', 'Date', 'Time']] = df_raw['Location_Date_Time'].apply(lambda x: pd.Series(split_location_date_time(x)))
    df_raw.drop(columns=['Location_Date_Time'], inplace=True)

    df_raw["Description"] = df_raw["Description"] + " " + df_raw["Source"]
    df_raw.drop(columns=['Source'], inplace=True)

    df_raw.rename(columns={'Time': 'Start_time'}, inplace=True)
    df_raw["End_time"] = "N/A"

    df_raw['Date'] = df_raw['Date'].str[-10:]
    df_raw.rename(columns={'Date': 'Start_date'}, inplace=True)
    df_raw["End_date"] = df_raw["Start_date"]

    df_raw["City"] = df_raw["Location"]

    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

def split_location_date_time(value):
    parts = value.split(',')
    if len(parts) == 3:
        return parts[0].strip(), parts[1].strip(), parts[2].strip()
    elif len(parts) == 2:
        return parts[0].strip(), parts[1].strip(), "N/A"
    else:
        return value.strip(), "N/A", "N/A"
    

# Example usage

df_raw = scrape_eventim(30)
df_prep = preprocess_eventim(df_raw)
df_prep.to_csv("Scraped_Events_Eventim_HH_SH.csv")