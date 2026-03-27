from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp

spark = SparkSession.builder \
    .appName('CartsSlver') \
    .getOrCreate()

df = spark.read.parquet('data/bronze/carts/')

# Explode produtos dentro do carrinho
df_exploded = df.withColumn('product', explode('products'))

df_clean = df_exploded.select(
    col('id').alias('order_id'),
    col('userId').alias('user_id'),
    col('date').alias('order_date'),
    col('product.productId').alias('product_id'),
    col('product.quantity').alias('quantity'),
    current_timestamp().alias('processed_at')
)

df_clean.write.mode('overwrite').parquet('data/silver/orders/')