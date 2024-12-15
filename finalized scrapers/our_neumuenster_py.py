from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz         # to set the German time zone.
import time as tm   # be careful, I specifically gave an alias as 'tm', cause I have a variable called 'time' already in my code.
import re

# pip install selenium
# pip install pytz
# or maybe this is needed: locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')

def scraping_neumuenster():
    driver = webdriver.Chrome()
    driver.get("https://www.neumuenster.de/kultur-freizeit/veranstaltungskalender") 

    germany_tz = pytz.timezone('Europe/Berlin')
    current_date = datetime.now(germany_tz).date() # Get the current date and time in the German time zone

    ten_days_from_today = current_date + timedelta(days=10)

    event_list = []  # to store all the events.

    is_true = True
    while is_true:  
        
        # Wait until the event containers are loaded
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.col-xs-10.col-sm-9.col-md-10')))
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'meta[itemprop="startDate"]')))
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'span[itemprop="address"]')))  
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'h5.dfx-titel-liste-dreizeilig'))) 
        # After loading, get the page source and parse it with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Now use BeautifulSoup to extract the events
        events = soup.select('div.col-xs-10.col-sm-9.col-md-10')  # Adjust selector to match event containers

        # Function to extract element's text or attribute using BeautifulSoup
        def get_element_or_none(soup, selector, attribute=None):
            try:
                element = soup.select_one(selector)
                if element:
                    if attribute:
                        return element.get(attribute).strip()  
                    else:
                        return element.text.strip() 
                return np.nan
            except:
                return np.nan

        # Loop through each event and extract details
        for event in events:

            # Check if the event date is within the next 10 days
            temp_date = event.select_one('meta[itemprop="startDate"]').get('content')
            check_date = datetime.fromisoformat(temp_date).date()

            if ten_days_from_today < check_date:
                is_true = False
                break  # Exit the loop if the event is more than 10 days away

            # Extracting event details
            title = get_element_or_none(event, 'h5.dfx-titel-liste-dreizeilig')
            date = get_element_or_none(event, 'meta[itemprop="startDate"]', attribute='content')
            time = get_element_or_none(event, 'span.dfx-zeit-liste-dreizeilig')
            address = get_element_or_none(event, 'span[itemprop="address"]')
            
            place_name = get_element_or_none(event, 'span[itemprop="name"]')
            full_address = f"{place_name} | {address}"
            source = get_element_or_none(event, 'h5.dfx-titel-liste-dreizeilig a', attribute='href')

            # Creating the event dictionary
            our_event = {
                'Subject': title,
                'Start_date': date,
                'time': time,
                'Location': full_address,
                'Description': source
            }

            event_list.append(our_event)

        if not is_true:
            break

        pagination_block = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'ul.pagination li')))
        page_link = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(pagination_block[-1].find_element(By.TAG_NAME, 'a')))
        page_link.click()
        tm.sleep(1)

    driver.close()  # Close the Selenium driver
    return pd.DataFrame(event_list)

df = scraping_neumuenster()

def cleaning_neumuenster(df):
    df['Start_date'] = df['Start_date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d') if 'T' in x else x)

    def func(x):
        if pd.isna(x):  # Check if x is NaN.
            return x    # Return NaN without processing.
        else:
            if 'bis' in x:
                x = x.replace('bis', '-')       # Replace 'bis' with a '-' hyphen sign 
            x = re.sub(r'[^0-9:-]', '', x)      # Keep only digits, colons, and hyphens. Remove the rest
        return x

    df['time'] = df['time'].apply(func)         # Apply the function to the 'time' column

    def split_time(time_str):
        if pd.isna(time_str):
            return np.nan, np.nan               # Return NaN if the input is NaN.
        elif '-' in time_str:
            start, end = time_str.split('-')
            return start.strip(), end.strip()   # Return start and end times.
        else:
            return time_str.strip(), np.nan     # Single time: end_time as NaN.

    df[['Start_time', 'End_time']] = df['time'].apply(split_time).apply(pd.Series)  # Apply the function to create two new columns

    df.drop(columns=['time'], inplace=True)
    df['End_date'] = df['Start_date']           # adding End_date.
    df['City'] = 'Neumünster'                   # adding city column.
    df['Category'] = ' '                        # adding category column.
    df['Music_label'] = False

    return df

cleaned_neumuenster = cleaning_neumuenster(df)