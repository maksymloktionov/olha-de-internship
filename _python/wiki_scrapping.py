from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

class WikiParser:
    def __init__(self, url):
        self.url = url
        logging.info('WikiParser instance is created')
    
    @staticmethod
    def get_page(url):
        try:
            page = requests.get(url)
            soup = BeautifulSoup(page.text, 'html.parser')
            return soup
        except requests.RequestException as e:
            logging.error(f'Error fetching page {e}')
            return None
    
    def get_tables(self, soup):
        return soup.find_all('table', class_='wikitable')
    
    def parse_tables(self, soup, table_index = 8):
        tables = self.get_tables(soup)
        rus_fed_table = tables[table_index]

        headers = self.extract_headers(rus_fed_table)
        data = []

        column_data = rus_fed_table.find_all('tr')
        for row in column_data[1:]:
            row_data = self.parse_row(row)
            data.append(row_data)
            
        self.df = pd.DataFrame(data, columns=headers)
        return self.df
    
    def extract_headers(self, table):
        headers = [header.get_text(strip=True) for header in table.find_all('th')]
        logging.info(f'Parsed headers: {headers}')
        return headers
    @staticmethod
    def parse_row(row):
        row_data = row.find_all('td')
        each_row = [data.text.strip() for data in row_data]
        return each_row

    def clean_data(self):
       logging.info('Cleaning data')
       
       self.df[["start_date", "end_date"]] = self.df["Date"].str.split("–", n=1, expand=True)
       self.df.replace({np.nan: None}, inplace=True)
       self.df.columns = self.df.columns.str.replace(' ', '_').str.lower()
       self.df['end_date'] = self.df['end_date'].replace(['present', "present[49]", "present[50]", 'None', pd.NA, np.nan], np.nan)
       
       logging.info('Finished cleaning data')

    def save_to_db(self, table_name):
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
            logging.error(f'Error saving to database: {e}')

    def run(self, table_name, table_index=8):
        soup = self.get_page(self.url)
        if soup:
            self.parse_tables(soup, table_index)
            self.clean_data()
            self.save_to_db(table_name)

if __name__ == "__main__":
    url = 'https://en.wikipedia.org/wiki/List_of_wars_involving_Russia#Russian_Federation_(1991%E2%80%93present)'
    scraper = WikiParser(url)   
    scraper.run(table_index=8, table_name='warcrimes')
