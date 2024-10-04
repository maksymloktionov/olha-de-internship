from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime
import numpy as np

import logging


logging.basicConfig(filename='wikiscript.log',level=logging.INFO)

class WikiParseWar:
    def __init__(self,url):
        self.url = url
        self.soup = None
        self.df = pd.DataFrame()

    def getPage(self):
        try:
            page = requests.get(self.url)
            self.soup = BeautifulSoup(page.text,'html')
        except requests.RequestException as e:
            logging.error (f'Error fetching page {e}')

    def getTables(self):
        tables = self.soup.find_all('table',class_='wikitable')
        
        return tables
    
    def parseTables(self,table_index):
        tables = self.getTables()
        rus_fed_table = tables[table_index]

        headers = [header.get_text(strip=True) for header in rus_fed_table.find_all('th')]
        logging.info(f"Parsed headers: {headers}")

        df = pd.DataFrame(columns=headers)

        column_data = rus_fed_table.find_all('tr')
        for row in column_data[1:]:
            row_data = row.find_all('td')
            each_row = [data.text.strip() for data in row_data]
            #print(each_row)

            length = len(df)
            df.loc[length] = each_row

            self.df = df
        return self.df
    
    def pdClean(self):
       logging.info('Cleaning data')
       
       self.df[["Start date","End date"]] = self.df["Date"].str.split("–",n=1,expand=True)
       self.df.replace({np.nan:None},inplace=True)
       self.df.columns = self.df.columns.str.replace(' ','_').str.lower()
       self.df['end_date'] = self.df['end_date'].replace(['present',"present[49]","present[50]" ,'None', pd.NA, np.nan], np.nan)
       
       logging.info('Finished cleaning data')

    def csvSave(self,csv_path):
        logging.info(f"Saving data to CSV: {csv_path}")
        self.df.to_csv(csv_path,index=False)  
        logging.info('Saved')

    def dbCon(self,table_name,db_url):
        logging.info(f'Saving data to database:{table_name}')
        engine = create_engine(db_url)
        self.df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False
        )

scraper = WikiParseWar('https://en.wikipedia.org/wiki/List_of_wars_involving_Russia#Russian_Federation_(1991%E2%80%93present)')   
scraper.getPage() 
scraper.parseTables(8) 
scraper.pdClean()  
scraper.csvSave('/home/olha/olha-de-internship/russianwars.csv') 
scraper.dbCon('warcrimes','postgresql://postgres:password@localhost:5544/postgres')
