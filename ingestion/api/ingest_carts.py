import pandas as pd
import os
from datetime import datetime
from client import fetch_data

BASE_PATH = os.getenv('BASE_PATH', 'data')
RAW_PATH = os.path.join(BASE_PATH, 'bronze', 'carts')

def ingest():
    data = fetch_data('carts')

    df = pd.json_normalize(
        data,
        record_path='products',
        meta=['id', 'userId', 'date']
    )

    # 🔥 Padronização de colunas
    df = df.rename(columns={
        'id': 'cart_id',
        'userId': 'user_id',
        'productId': 'product_id'
    })

    df['ingestion_date'] = datetime.utcnow()

    os.makedirs(RAW_PATH, exist_ok=True)

    file_name = f'carts_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.parquet'

    df.to_parquet(os.path.join(RAW_PATH, file_name), index=False)

    print(f'[OK] Carts ingeridos: {file_name}')

if __name__ == '__main__':
    ingest()