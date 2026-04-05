import pandas as pd
import os
from datetime import datetime
from client import fetch_data

BASE_PATH = os.getenv('BASE_PATH', 'data')
RAW_PATH = os.path.join(BASE_PATH, 'bronze', 'products')

def ingest():
    data = fetch_data('products')

    df = pd.json_normalize(data)

    df['ingestion_date'] = datetime.utcnow()

    os.makedirs(RAW_PATH, exist_ok=True)

    file_name = f'products_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.parquet'

    df.to_parquet(os.path.join(RAW_PATH, file_name), index=False)

    print(f'[OK] Products ingeridos: {file_name}')

if __name__ == '__main__':
    ingest()