import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

file_names = ['kyiv2020-2023.csv', 'munich2020-2023.csv', 'oslo2020-2023.csv','paris2020-2023.csv','zagreb2020-2023.csv','helsinki2020-2023.csv','athens2020-2023.csv']


merged_data = pd.DataFrame()

city_names = ['Kyiv', 'Munich', 'Oslo', 'Paris', 'Zagreb','Helsinki','Athens']

for filename,city in zip(file_names,city_names):
    df = pd.read_csv(filename)
    df['city'] = city

    merged_data = pd.concat([merged_data, df], ignore_index=True)


merged_data.to_csv('stg_cities.csv', index=False)

# Convert the CSV to Parquet
table = pa.Table.from_pandas(merged_data)
pq.write_table(table, 'stg_cities.parquet')


parquet_file = 'stg_cities.parquet'
table = pq.read_table(parquet_file)
print(table.schema)

