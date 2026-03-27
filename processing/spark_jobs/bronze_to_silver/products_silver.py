from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder \
    .appName('ProductsSlver') \
    .getOrCreate()

df = spark.read.parquet('date/bronze/products')

df_clean = df.select(
    col('id').alias('product_id'),
    col('title'),
    col('price').cast('double'),
    col('category'),
    col('description'),
    col('image'),
    current_timestamp().alias('processed_at')
).dropDuplicates(['product_id'])

df_clean.write.mode('overwrite').parquet('data/silver/products/')