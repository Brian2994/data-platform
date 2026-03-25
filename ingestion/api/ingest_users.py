import pandas as pd
import os
from datetime import datetime
from client import fetch_data

RAW_PATH = 'data/bronze/users'

def ingest():
    data = fetch_data('users')

    df = pd.json_normalize(data)

    df['ingestion_date'] = datetime.now()

    os.makedirs(RAW_PATH, exist_ok=True)

    file_name = f'users_{datetime.now().strftime("%Y%m%d%H%M%S")}.parquet'

    df.to_parquet(os.path.join(RAW_PATH, file_name), index=False)

    print(f'Users ingeridos: {file_name}')

if __name__ == '__main__':
    ingest()