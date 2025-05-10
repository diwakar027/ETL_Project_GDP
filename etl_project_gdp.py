import requests
import pandas as pd
from bs4 import BeautifulSoup
import lxml
import numpy as np
from datetime import datetime
import mysql.connector

URL = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
CSV_PATH = 'Countries_by_GDP.csv'
SQL_CONNECTION = ['127.0.0.1', 'root', '112104', '6666']
LOG_PATH = 'log_file.text'
TABLE_ATTRIBS = ['Country','GDP_USD_millions']
DB_NAME = 'World_Economies'
TABLE_NAME = 'Countries_by_GDP'


def extract(url, table_attribs):
    webpage = requests.get(url).text
    soup = BeautifulSoup(webpage,'html.parser')
    
    df = pd.DataFrame(columns=table_attribs)
    
    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows :
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns={"GDP_USD_millions":"GDP_USD_billions"})
    return df
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def connect_database(sql_connection):
    
    mydb = mysql.connector.connect(
        
    host=sql_connection[0],
    user=sql_connection[1],
    password=sql_connection[2],
    port=sql_connection[3]
    )   
    return mydb
    
def load_to_db(df):
    db = connect_database(SQL_CONNECTION)
    cursor = db.cursor()
    column_names = df.columns.tolist()
    data_records = df.to_records(index=False)

    # Create the schema and table
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{DB_NAME}`")
    cursor.execute(f"USE `{DB_NAME}`")
    columns_sql = ', '.join([f"`{col}` VARCHAR(255)" if df[col].dtype == 'object' else f"`{col}` FLOAT" for col in column_names])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (id INT AUTO_INCREMENT PRIMARY KEY, {columns_sql})")

    # Prepare the INSERT statement
    placeholders = ', '.join(['%s'] * len(column_names))
    DML = f"INSERT INTO `{TABLE_NAME}` ({', '.join([f'`{col}`' for col in column_names])}) VALUES ({placeholders})"

    # Insert each record
    for record in data_records:
        values = tuple(record[col] for col in column_names)
        cursor.execute(DML, values)

    db.commit()
    db.close()


def run_query(query_statement):
    db = connect_database(SQL_CONNECTION)
    cursor = db.cursor()
    cursor.execute(f"Use {DB_NAME}")
    print(pd.read_sql(query_statement,db))
    return query_statement
    
def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(LOG_PATH,"a") as f:
        f.write(timestamp+","+message+"\n")


try:
    df=extract(URL,TABLE_ATTRIBS)
    log_progress("Data extracted successfuly")
except Exception as e:
    log_progress(f"Error while extracting data: {e}")

try:       
    df=transform(df)
    log_progress("Data transformed successfuly")
except Exception as e:
    log_progress(f"Error while transforming data: {e}")    

try:       
    load_to_csv(df,CSV_PATH)
    log_progress("Data loaded to CSV successfuly")
except Exception as e:
    log_progress(f"Error while loading data to CSV: {e}")   

try:       
    load_to_db(df)
    log_progress("Data loaded to DB successfuly")
except Exception as e:
    log_progress(f"Error while loading data to DB: {e}")   

try:       
    quey_statement = run_query(f"SELECT * FROM {TABLE_NAME}")
except Exception as e:
    log_progress(f"Error while querying data: {e}")
finally:
        log_progress(f"Query made: {quey_statement}")
           

