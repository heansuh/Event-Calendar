# Script for Meine Stadt (HH, SH) Event Calendar
# The functions scrape_meine_stadt() and preprocess_meine_stadt() should be imported to the script defining the main scraping process

# Imports

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import pandas as pd
from datetime import datetime
import time


# Scraping function

def scrape_meine_stadt(): #ein aufruf dauert etwa 30 min, weil die website langsam lädt und es sehr viele passende events gibt

    urls = [
        "https://veranstaltungen.meinestadt.de/hamburg/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/hamburg/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/hamburg/festivals/alle",
        "https://veranstaltungen.meinestadt.de/kiel/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/kiel/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/kiel/festivals/alle",
        "https://veranstaltungen.meinestadt.de/luebeck/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/luebeck/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/luebeck/festivals/alle",
        "https://veranstaltungen.meinestadt.de/flensburg/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/flensburg/partys-feiern/alle", 
        "https://veranstaltungen.meinestadt.de/flensburg/festivals/alle",
        "https://veranstaltungen.meinestadt.de/husum-nordsee/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/husum-nordsee/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/husum-nordsee/festivals/alle",
        "https://veranstaltungen.meinestadt.de/heide/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/heide/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/heide/festivals/alle",
        "https://veranstaltungen.meinestadt.de/schleswig/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/schleswig/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/schleswig/festivals/alle",
        "https://veranstaltungen.meinestadt.de/itzehoe/konzerte/alle",
        "https://veranstaltungen.meinestadt.de/itzehoe/partys-feiern/alle",
        "https://veranstaltungen.meinestadt.de/itzehoe/festivals/alle"
    ] 
    
    # preparations
    options = Options()
    options.add_argument("--headless")  # run in headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")  # avoid some rendering issues
    options.add_argument("--window-size=1920,1080")  # avoid window size issues
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36") #avoid being blocked as an automatic scraper
    options.add_argument("--log-level=3")  # suppress most logs
    driver = webdriver.Chrome(service=Service(), options=options)
    #driver = webdriver.Firefox()
    wait = WebDriverWait(driver, 10) 

    events = []

    for url in urls:
        driver.get(url)

        # cookie acceptance
        try:
            iframe = wait.until(EC.presence_of_element_located((By.ID, "sp_message_iframe_1220563")))
            driver.switch_to.frame(iframe)
            buttons = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "button-responsive-primary")))
            buttons[1].click()
        except Exception as e:
            #print(f"An error occurred: {e}")
            print("Cookie rejection in iframe ging nicht. Macht aber nix.")

        # move back from cookie iframe 
        driver.switch_to.default_content()

        for i in range(30): #daumenregel für hh konzerte (, rest lädt dann ggf zu viel) #30 TODO wieder auf 30
            time.sleep(10) #website lädt leider sehr langsam
            try: #weitere events laden
                wait = WebDriverWait(driver, 10)
                load_more_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@data-component="CsSecondaryButton"]')))
                load_more_button.click()
            except Exception as e:
                #print(f"An error occurred: {e}")
                print("Mehr laden ging nicht. Ist ok.") #TODO löschen
                break

        elements = driver.find_elements(By.CSS_SELECTOR, 'div.flex.flex-col.w-full.p-16.screen-m\\:pl-0')
        print("gefundene events:", str(len(elements)))

        # Loop through each element and extract the required information
        for element in elements:
            
            try:
                title = element.find_element(By.CSS_SELECTOR, 'h3.text-h3.font-bold-headline.mb-8.line-clamp-2').text
            except:
                title = ' '
            
            try:
                source = element.find_element(By.CSS_SELECTOR, 'a.ms-clickArea').get_attribute('href')
            except:
                source = ' '
            
            try:
                date_time = element.find_element(By.CSS_SELECTOR, 'div.flex.mb-4.text-h4').text
            except:
                date_time = ' '
            
            try:
                city_location = element.find_element(By.CSS_SELECTOR, 'div.flex.mb-8.text-h4').text
            except:
                city_location = ' '
            
            event = {
                "Subject": title,
                "Description": source, 
                "Date_Time": date_time,
                "City_Location": city_location,
                "Category": url.split(('/'))[-2],
                "Music_label": True
            }
            events.append(event)

    df_raw = pd.DataFrame(events)
    driver.close()
    print(df_raw.head())
    print(df_raw.shape)
    return df_raw


# Preprocessing function

def preprocess_meine_stadt(df_raw):

    #df_raw[["Start_date", "Start_time"]] = df_raw["Date_Time"].apply(lambda x: pd.Series(process_date_time(x)))
    df_raw["Start_date"] = df_raw["Date_Time"].apply(preprocess_date)
    df_raw["Start_time"] = df_raw["Date_Time"].apply(preprocess_time)
    df_raw["Start_date"] = df_raw["Start_date"].apply(convert_date_format)
    df_raw["End_date"] = df_raw["Start_date"]
    df_raw["End_time"] = " "
    df_raw.drop(columns=["Date_Time"], inplace= True)

    #df_raw[["City", "Location"]] = df_raw["City_Location"].apply(lambda x: pd.Series(process_city_location(x)))
    df_raw["City"] = df_raw["City_Location"].apply(preprocess_city)
    df_raw["Location"] = df_raw["City_Location"].apply(preprocess_location)
    df_raw.drop(columns=["City_Location"], inplace= True)
    
    df_raw = df_raw.fillna(" ")
    df_prep = df_raw[['Subject','Start_date', 'End_date', 'Start_time', 'End_time', 'Location', 'City', 'Description', 'Category', 'Music_label']]
    return df_prep


# Helper functions and elements

"""def process_date_time(date_time_string):
    date = date_time_string.split(" ")[1][:-1]
    time = date_time_string.split(" ")[2]
    return date, time"""

def preprocess_date(date_time_string):
    if len(date_time_string.split(" ")) > 1:
        date = date_time_string.split(" ")[1][:-1]
    else:
        date = " "
    return date

def preprocess_time(date_time_string):
    if len(date_time_string.split(" ")) > 2:
        time = date_time_string.split(" ")[2]
    else:
        time = " "
    return time

def convert_date_format(date_str):
    date_str = str(date_str)
    if "." in date_str:
        return pd.to_datetime(date_str, format='%d.%m.%Y').strftime('%Y-%m-%d')
    else:
        return " "
    
"""def process_city_location(citlocstr):
    print(citlocstr)
    city = citlocstr.split(", ", 1)[0]
    location = citlocstr.split(", ", 1)[1]
    return city, location"""

def preprocess_city(citlocstr):
    city = citlocstr.split(", ", 1)[0]
    return city

def preprocess_location(citlocstr):
    if len(citlocstr.split(", ", 1)) == 2:
        location = citlocstr.split(", ", 1)[1]
    else:
        location = " "
    return location


# Example usage

df_raw = scrape_meine_stadt()
#df_raw.to_csv("Meine_Stadt_Test.csv")
df_prep = preprocess_meine_stadt(df_raw)
df_prep.to_csv("Scraped_Events_Meine_Stadt.csv")
print(df_prep.head())
print(df_prep.info())

#unsupported pixel und handshake fail sind egal