# %%
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime
import numpy as np

# %%
url = 'https://en.wikipedia.org/wiki/List_of_wars_involving_Russia#Russian_Federation_(1991%E2%80%93present)'
page = requests.get(url)
soup = BeautifulSoup(page.text, 'html')

# %%
print(soup)

# %%
tables = soup.find_all('table', class_='wikitable')

# %%
rus_fed_table = tables[8]

# %%
print(rus_fed_table)

# %%
headers = [header.get_text(strip=True) 
           for header in rus_fed_table.find_all('th')]
print(headers)

# %%
df = pd.DataFrame(columns= headers)
df

# %%
column_data = rus_fed_table.find_all('tr')
print(column_data)

# %%
for row in column_data[1:]:
    row_data = row.find_all('td')
    each_row = [data.text.strip() for data in row_data]
    #print(each_row)

    length = len(df)
    df.loc[length] = each_row

# %%

df[["Start date","End date"]] = df["Date"].str.split("–",n=1,expand=True)
df

# %%
df.to_csv(r'/home/olha/repos/olha-de-internship/russianwars.csv')

# %%
df.replace({np.nan: None}, inplace=True)

# %%
df.columns = df.columns.str.replace(' ','_').str.lower()

# %%
df['end_date'] = df['end_date'].replace(['present',"present[49]","present[50]" ,'None', pd.NA, np.nan], np.nan)

# %%
df

# %%
engine = create_engine('postgresql://postgres:password@localhost:5544/postgres')

df.to_sql(
    name="warcrimes",
    con=engine,
    if_exists="append",
    index=False
)


