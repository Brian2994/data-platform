from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, coalesce, lit
import os

BRONZE_PATH = "data/bronze/products/"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/data_platform"

def main():
    print("🚀 Iniciando Spark...")

    spark = SparkSession.builder \
        .appName('ProductsSilver') \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    # 🔹 Verifica se existe dado
    if not os.path.exists(BRONZE_PATH) or len(os.listdir(BRONZE_PATH)) == 0:
        print("❌ Nenhum arquivo encontrado na camada bronze")
        return

    print("📥 Lendo dados bronze...")

    df = spark.read.parquet(BRONZE_PATH)

    print(f"🔍 Registros lidos: {df.count()}")

    # 🔹 Transformação (Silver)
    df_clean = df.select(
        col('id').alias('product_id'),

        # 🔥 Tratamento de strings
        coalesce(col('title'), lit('unknown')).alias('title'),

        # 🔥 Tratamento de preço
        coalesce(col('price').cast('double'), lit(0.0)).alias('price'),

        coalesce(col('category'), lit('unknown')).alias('category'),
        coalesce(col('description'), lit('')).alias('description'),
        coalesce(col('image'), lit('')).alias('image'),

        current_timestamp().alias('processed_at')
    ).dropDuplicates(['product_id'])

    print(f"✅ Registros após limpeza: {df_clean.count()}")

    # 🔹 Escrita no Postgres
    try:
        print("💾 Salvando arquivo...")

        df_clean.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "products") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .mode("overwrite") \
            .save()

        print("🎉 Users salvo com sucesso!")

    except Exception as e:
        print("❌ Erro ao salvar:")
        print(e)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()