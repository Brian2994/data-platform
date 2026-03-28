from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, concat_ws, current_timestamp

spark = SparkSession.builder \
    .appName('UsersSilver') \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet('/opt/project/data/bronze/users/')

df_clean = df.select(
    col('id').alias('user_id'),
    concat_ws(' ', col('name.firstname'), col('name.lastname')).alias('full_name'),
    col('email'),
    col('phone'),
    col('address.city').alias('city'),
    col('address.zipcode').alias('zipcode'),
    current_timestamp().alias('processed_at')
).dropDuplicates(['user_id'])

df_clean.write.mode('overwrite').parquet('/opt/project/data/silver/users/')