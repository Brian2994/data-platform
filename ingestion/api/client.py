import requests

BASE_URL = 'https://fakestoreapi.com'

def fetch_data(endpoint):
    url = f'{BASE_URL}/{endpoint}'
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f'Erro na API: {response.status_code}')
    
    return response.json()