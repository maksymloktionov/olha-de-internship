from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


class WikiParser:
    def __init__(self, table_index=8):
        self.url = "https://en.wikipedia.org/wiki/List_of_wars_involving_Russia#Russian_Federation_(1991%E2%80%93present)"
        self.table_index = table_index
        logging.info('WikiParser instance is created')

    @staticmethod
    def get_page(url):
        try:
            page = requests.get(url)
            page.raise_for_status()
            return BeautifulSoup(page.text, 'html.parser')
        except requests.RequestException as e:
            logging.error(f'Error fetching page: {e}')
            return None

    @staticmethod
    def get_tables(soup):
        return soup.find_all('table', class_='wikitable')

    def parse_tables(self, soup):
        tables = self.get_tables(soup)
        if self.table_index >= len(tables):
            logging.error(f'Table at index {self.table_index} not found.')
            return pd.DataFrame()

        rus_fed_table = tables[self.table_index]
        headers = self.extract_headers(rus_fed_table)
        data = [self.parse_row(row) for row in rus_fed_table.find_all('tr')[1:]]

        return pd.DataFrame(data, columns=headers)

    @staticmethod
    def extract_headers(table):
        headers = [header.get_text(strip=True) for header in table.find_all('th')]
        logging.info(f'Parsed headers: {headers}')
        return headers

    @staticmethod
    def parse_row(row):
        return [data.text.strip() for data in row.find_all('td')]

    @staticmethod
    def clean_data(df):
        logging.info('Cleaning data')

        df[["start_date", "end_date"]] = df["Date"].str.split("–", n=1, expand=True)
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df['end_date'] = pd.to_datetime(df['end_date'].replace(['present', "present[49]", "present[50]", None], np.nan),
                                        errors='coerce')

        df.columns = df.columns.str.replace(' ', '_').str.lower()

        logging.info('Finished cleaning data')
        return df

    @staticmethod
    def save_to_db(df, table_name):
        logging.info(f'Saving data to database: {table_name}')
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            logging.error('DATABASE_URL is not set.')
            return

        try:
            with create_engine(database_url).connect() as connection:
                df.to_sql(name=table_name, con=connection, if_exists="replace", index=False)
            logging.info('Data saved successfully')
        except Exception as e:
            logging.error(f'Error saving to database: {e}')

    def run(self, table_name):
        soup = self.get_page(self.url)
        if soup:
            df = self.parse_tables(soup)
            if not df.empty:
                cleaned_df = self.clean_data(df)
                self.save_to_db(cleaned_df, table_name)


if __name__ == "__main__":
    scraper = WikiParser()
    scraper.run(table_name='war_crimes')
