from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import numpy as np
import os
import logging


logging.basicConfig(level=logging.INFO)

class WikiParseWar:
    def __init__(self,url):
        self.url = url
        self.df = pd.DataFrame()
        logging.info(f'DATABASE_URL: {os.getenv("DATABASE_URL")}')
    

    def getPage(self):
        try:
            page = requests.get(self.url)
            self.soup = BeautifulSoup(page.text,'html')
        except requests.RequestException as e:
            logging.error (f'Error fetching page {e}')

    def getTables(self):
        
        return self.soup.find_all('table',class_='wikitable')
    
    def parse_tables(self,table_index):
        tables = self.getTables()
        rus_fed_table = tables[table_index]

        headers = self.extract_headers(rus_fed_table)
        data = []

        column_data = rus_fed_table.find_all('tr')
        for row in column_data[1:]:
            row_data = self.parse_row(row)
            data.append(row_data)
            
        self.df = pd.DataFrame(data,columns=headers)
        return self.df
    
    def extract_headers(self,table):
        headers = [header.get_text(strip=True) for header in table.find_all('th')]
        logging.info(f'Parsed headers:{headers}')
        return headers

    def parse_row(self,row):
        row_data = row.find_all('td')
        each_row = [data.text.strip() for data in row_data]
        return each_row
    

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

    def dbCon(self, table_name):
        logging.info(f'Saving data to database: {table_name}')
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            logging.error('DATABASE_URL is not set.')
            return 
        engine = create_engine(database_url)
        self.df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False
        )
        logging.info('Saved')

   
        


if __name__ == "__main__":
        scraper = WikiParseWar('https://en.wikipedia.org/wiki/List_of_wars_involving_Russia#Russian_Federation_(1991%E2%80%93present)')   
        scraper.getPage() 
        scraper.parse_tables(8) 
        scraper.pdClean()
        scraper.csvSave('/home/olha/olha-de-internship/file.csv') 
        scraper.dbCon('warcrimes')

