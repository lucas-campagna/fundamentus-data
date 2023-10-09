import pandas as pd

import requests
from bs4 import BeautifulSoup

from .page import get_page
from .utils import build_rawdata

def get_prices(asset):
    _, global_vars = get_page(asset)
    data = build_rawdata({
        'cod_negociacao',
        'codcvm',
        'especi',
        'ISIN',
    },
        global_vars
    )
    headers = get_prices.headers.copy()
    headers['cookie'] = f'SESSION_ID={global_vars["session_id"]}; _gid=GA1.3.109950653.1696418963; __gads=ID=677dbc9532f65f4d-222656e2a8b4007c:T=1686851767:RT=1696445869:S=ALNI_MalXYE5YMlaeg7ClHEgXeL0Opf7mg; __gpi=UID=00000c4f7016ee69:T=1686851767:RT=1696445869:S=ALNI_MZT9LEQVYq8Jqct4fiIvvnUVw-1-A; _ga=GA1.1.1931325936.1686851770; _ga_LCCM64WERB=GS1.1.1696445022.49.1.1696445904.0.0.0'
    headers['csrf-token'] = global_vars['token']
    headers['referer'] = f'https://www.investsite.com.br/principais_indicadores.php?cod_negociacao={asset}'
    response = requests.post('https://www.investsite.com.br/includes/grafico_cotacoes_ajax.php',headers=headers,data=data)
    assert(response.status_code == 200)    
    prices = pd.DataFrame(response.json()).set_index('datascotacoes')
    prices.index = pd.to_datetime(prices.index,format='%Y%m%d')
    return prices

get_prices.headers = {
    'authority': 'www.investsite.com.br',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://www.investsite.com.br',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.47',
}