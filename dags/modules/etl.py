import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()  # Load .env file

spark = SparkSession.builder \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
        .master("local") \
        .appName("PySpark_Postgres").getOrCreate()

jdbc_url = os.getenv("DB_URL") 
db_properties = {
        "driver": "org.postgresql.Driver",
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD")
}

def read_table_to_views(tables):
    # Turn into temporary views
    for table in tables:
        df = spark.read.jdbc(url=jdbc_url, table=table, properties=db_properties)
        df.createOrReplaceTempView(table)

def load_data_to_postgres(parquet_path, table_name):
    df = pd.read_parquet(parquet_path)

    engine = create_engine(os.getenv("CONN"),echo=False)
    df.to_sql(name=table_name, con=engine, if_exists='append')

def extract_transform_top_countries():
    tables = ["city", "country", "customer", "address"]

    read_table_to_views(tables)

    # Top Country 
    df_result = spark.sql('''
        SELECT
            country,
            COUNT(country) as total,
            current_date() as date,
            'vian' as data_owner
        FROM customer
        JOIN address ON customer.address_id = address.address_id
        JOIN city ON address.city_id = city.city_id
        JOIN country ON city.country_id = country.country_id
        GROUP BY country
        ORDER BY total DESC
        ''')
    
    df_result.write.mode('overwrite') \
      .partitionBy('date') \
      .option('compression', 'snappy')\
      .option('partitionOverwriteMode', 'dynamic') \
      .save('data_result_1')

def extract_transform_total_film_by_category():
    tables = ["film", "film_category", "category"]

    read_table_to_views(tables)

    df_result_2 = spark.sql('''
        select 
            c.name AS Category_Name,
            COUNT(f.film_id) AS Total_Film,
        CURRENT_DATE() AS date,
        'vian' AS data_owner
        from
            film f
        inner join 
            film_category fc using(film_id)
        inner join
            category c using(category_id)
        group by 1
        order by 2 desc
        ''')
    
    df_result_2.write.mode('overwrite') \
      .partitionBy('date') \
      .option('compression', 'snappy')\
      .option('partitionOverwriteMode', 'dynamic') \
      .save('data_result_2')