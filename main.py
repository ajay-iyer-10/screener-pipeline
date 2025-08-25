from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import os
from dotenv import load_dotenv
import json
load_dotenv()
EMAIL = 'jahol58040@dariolo.com'
PASSWORD = 'fakepassword@2'
DB_PASSWORD = 'mypassword'
DB_USER =  'postgres'
DB = 'mydatabase'
def getdata(url):
    r = requests.get(url)
    return r.text

def format_header(uncleaned_list):
    cleaned_list = []
    for param in uncleaned_list:
        clean_string = param.strip().replace(' ','_')
        cleaned_list.append(clean_string)
    return cleaned_list

def remove_duplicates(list_with_dupes):
    list_without_dupes = list(dict.fromkeys(list_with_dupes))
    return list_without_dupes

def scrape_data(url, tag):
    htmldata = getdata(url)
    soup = BeautifulSoup(htmldata, 'html.parser')
    data = ''
    data_list = []
    for data in soup.find_all(tag):
        data_list.append(data.get_text(strip = True))
    return data_list

def chunked_data(flat_list,no_of_cols):
    chunks = [flat_list[x:x+no_of_cols] for x in range(0, len(flat_list), no_of_cols)]
    return chunks

def data_to_df(header_list,data_list):
    df = pd.DataFrame(data_list, columns=header_list)
    return df

def df_to_posgres(df):
    engine = create_engine(
        f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@localhost:5432/{DB}')
    df.to_sql('pipeline_test', engine, if_exists='replace',index=False)
    return 'Table pushed to postgres'

def read_table(table_name):
     engine = create_engine(
        f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@localhost:5432/{DB}')
     df = pd.read_sql_table(f'{table_name}',engine)
     return df

def save_json(df, table_name):
    result = df.to_json(orient="records")
    parsed = json.loads(result)
    with open (f'{table_name}.json','w') as f:
        json.dump(parsed, f, indent=4)
    print(f'{table_name}.json successfully created')
    

if __name__=='__main__':
    final_df = pd.DataFrame()
    # DRIVER_PATH = '/path/to/chromedriver'
    driver = webdriver.Chrome()

    driver.get("https://www.screener.in/login/?")
    driver.find_element(By.ID, 'id_username').send_keys(EMAIL)
    driver.find_element(By.ID, 'id_password').send_keys(PASSWORD)
    driver.find_element(By.XPATH, '//html/body/main/div[2]/div[2]/form/button').click()
    time.sleep(2)   
    driver.find_element(By.XPATH, '//html/body/div/div[2]/main/div[1]/div[1]/div/button').click()
    driver.find_element(By.XPATH, '//html/body/div/div[2]/main/div[1]/div[1]/div/ul/li[2]/a').click()
    driver.find_element(By.XPATH, '//html/body/div/div[2]/main/div[1]/div[2]/a[1]').click()
    current_url = driver.current_url
    header_data=[]
    headers = driver.find_elements(By.TAG_NAME,'th')
    for data in headers:
        header_data.append(data.text)
    column_data=[]
    data = driver.find_elements(By.TAG_NAME,'td')
    for data in data:
        column_data.append(data.text)
    unique_header_data = remove_duplicates(header_data)
    cleaned_column_data = list(filter(None,column_data))
    chunked_columns = chunked_data(cleaned_column_data, len(unique_header_data))
    driver.close()
    df = data_to_df(unique_header_data, chunked_columns)
    df['load_dttm'] = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
    confirmation = df_to_posgres(df)
    print(confirmation)












