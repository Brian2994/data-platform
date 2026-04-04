from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, coalesce, lit
import os

BRONZE_PATH = "data/bronze/users/"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/data_platform"

def main():
    print("🚀 Iniciando Spark...")

    spark = SparkSession.builder \
        .appName('UsersSilver') \
        .master("local[*]") \
        .config("spark.jars", "/opt/project/jars/postgresql-42.7.3.jar") \
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
        col('id').alias('user_id'),
        concat_ws(
            ' ',
            coalesce(col('`name.firstname`'), lit('')),
            coalesce(col('`name.lastname`'), lit(''))
        ).alias('full_name'),
        col('email'),
        col('phone'),
        coalesce(col('`address.city`'), lit('unknown')).alias('city'),
        coalesce(col('`address.zipcode`'), lit('unknown')).alias('zipcode'),
        current_timestamp().alias('processed_at')
    ).dropDuplicates(['user_id'])

    print(f"✅ Registros após limpeza: {df_clean.count()}")

    # 🔹 Escrita no Postgres
    try:
        print("💾 Salvando arquivo...")

        df_clean.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "users") \
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