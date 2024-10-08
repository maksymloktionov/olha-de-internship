from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import numpy as np
import os
import logging


logging.basicConfig(level=logging.INFO)

class Wiki_ParseWar:
    #this is an instance method
    def __init__(self,url):
        self.url = url
        logging.info('Database connetion was establish successfully')
    
    @staticmethod
    def get_Page(url):
        try:
            page = requests.get(url)
            soup = BeautifulSoup(page.text,'html')
            return soup
        except requests.RequestException as e:
            logging.error (f'Error fetching page {e}')
            return None
    
    def get_Tables(self,soup):
        
        return soup.find_all('table',class_='wikitable')
    
    def parse_tables(self,soup,table_index):
        tables = self.get_Tables(soup)
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
    

    def pd_Clean(self):
       logging.info('Cleaning data')
       
       self.df[["Start date","End date"]] = self.df["Date"].str.split("–",n=1,expand=True)
       self.df.replace({np.nan:None},inplace=True)
       self.df.columns = self.df.columns.str.replace(' ','_').str.lower()
       self.df['end_date'] = self.df['end_date'].replace(['present',"present[49]","present[50]" ,'None', pd.NA, np.nan], np.nan)
       
       logging.info('Finished cleaning data')

    #def csv_Save(self,csv_path):
     #   logging.info(f"Saving data to CSV: {csv_path}")
      #  self.df.to_csv(csv_path,index=False)  
       # logging.info('Saved')

    def db_Con(self, table_name):
        logging.info(f'Saving data to database: {table_name}')
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            logging.error('DATABASE_URL is not set.')
            return 
        try:
            engine = create_engine(database_url)
            self.df.to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False
            )
            logging.info('Saved')
        except Exception as e:
            logging.error(f'Error saving to database:{e}')

   
    def run(self,table_index,table_name):
        soup = self.get_Page(self.url)
        if soup:
            self.parse_tables(soup,table_index)
            self.pd_Clean()
           # self.csv_Save(csv_path)
            self.db_Con(table_name)
if __name__ == "__main__":
    url = 'https://en.wikipedia.org/wiki/List_of_wars_involving_Russia#Russian_Federation_(1991%E2%80%93present'
    scraper = Wiki_ParseWar(url)   
    scraper.run(table_index=8,table_name='warcrimes')