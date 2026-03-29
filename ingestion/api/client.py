import requests
import time

BASE_URL = 'https://fakestoreapi.com'

def fetch_data(endpoint, retries=3, delay=2):
    url = f'{BASE_URL}/{endpoint}'

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                return response.json()

            else:
                print(f"[WARN] Status {response.status_code} na tentativa {attempt+1}")

        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Tentativa {attempt+1} falhou: {e}")

        time.sleep(delay)

    raise Exception(f"[FAIL] Não foi possível obter dados de {endpoint}")