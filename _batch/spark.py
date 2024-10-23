from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark import SparkConf
import time
import kagglehub

# Download the dataset
path = kagglehub.dataset_download("ashpalsingh1525/imdb-movies-dataset")

def create_spark_session() -> SparkSession:
    conf = SparkConf().set("spark.driver.memory", "8g")
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("IMDB movies") \
        .config(conf=conf) \
        .getOrCreate()
    return spark

def read_data(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, inferSchema=True)



if __name__ == '__main__':
    
    spark = create_spark_session()
    data_df = read_data(spark, path)

    data_df.show()
        # For UI to stick
    time.sleep(5)
