#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark import SparkConf
from pyspark.sql.types import StringType,IntegerType
from pyspark.sql.functions import col, count, isnan, when,desc,expr
#import time
#import pycountry
import kagglehub
import logging

# Download the dataset
path = kagglehub.dataset_download("ashpalsingh1525/imdb-movies-dataset")

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

def create_spark_session() -> SparkSession:
    conf = SparkConf()\
        .set("spark.driver.memory", "8g")\
        .set("spark.ui.port", "4040")
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("IMDB movies") \
        .config(conf=conf) \
        .getOrCreate()

    # Set the logging level for Spark
    spark.sparkContext.setLogLevel("WARN")  

    logging.info('Spark session was created')
    return spark

def read_data(spark: SparkSession, path: str) -> DataFrame:
    try:
        return spark.read.csv(path, header=True, inferSchema=True)
    except FileNotFoundError as e:
        logging.error(f'Error reading file: {e}')    
        return None 

def rename_columns(data_df: DataFrame) -> DataFrame:
    return data_df \
        .withColumnRenamed("names","Movie_name") \
        .withColumnRenamed("date_x","Release_date") \
        .withColumnRenamed("score","User_rating") \
        .withColumnRenamed("genre","Genre") \
        .withColumnRenamed("crew","Crew") \
        .withColumnRenamed("overview","Overview") \
        .withColumnRenamed("orig_title","Original_Title") \
        .withColumnRenamed("orig_lang","Original_language") \
        .withColumnRenamed("status","Status") \
        .withColumnRenamed("budget_x","Movie_Budget") \
        .withColumnRenamed("revenue","Revenue") \
        .withColumnRenamed("country","Country") \
        
def check_null_values(data_df: DataFrame) -> None:
    logging.info('Checking for null values in dataset')
    data_df.createOrReplaceTempView("data")
    null_values = data_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data_df.columns])
    null_values.show()
    return null_values

def delete_null_values(data_df: DataFrame) -> DataFrame:
    null_dropped = data_df.na.drop()
    cleaned_df = null_dropped.dropDuplicates()
    logging.info('Removed null values')

    cleaned_df = cleaned_df.filter(~col('Country').cast('string').rlike('^[0-9]*\.?[0-9]+$'))
    return cleaned_df

#def get_country_name(country_code):
 #   try:
  #      return pycountry.countries.get(alpha_2 = country_code).name
   # except:
    #    return None
#country_name_udf = udf(get_country_name,StringType())


def top_10_movie(data_df:DataFrame) -> DataFrame:
    return data_df.orderBy(desc("User_rating")).limit(10)
    

def save_to_csv(data_df: DataFrame, output_path: str) -> None:
    data_df.write.options(mode='overwrite',header='True').csv(output_path)
    logging.info(f'Saved DataFrame to CSV at {output_path}')


if __name__ == '__main__':
    spark = create_spark_session()
    data_df = read_data(spark, path)

    
    if data_df is not None:
        data_df = rename_columns(data_df)

        #data_df = data_df.withColumn("Country",country_name_udf(col("Country")))
        data_df.show()
        check_null_values(data_df)
        data_df = delete_null_values(data_df)
       # data_df.show()

        #data_df = analyze_data(data_df)
        #data_df.show()

        top_10_df = top_10_movie(data_df)
        top_10_df.select(
            expr("substring(Movie_name, 1, 50) AS Movie_name"),  # Show first 50 characters of Movie_name
            col('User_rating').cast('int').alias('User_rating'),
            'Genre',
            'Country'
        ).show(truncate=False) 
        csv_output_path = '/usr/src/app/imdb.csv'
        save_to_csv(data_df,csv_output_path)


        #data_df.select(['names', 'score', 'country']).show(20, truncate=False)
    # For UI to stick
    #time.sleep(1000)
