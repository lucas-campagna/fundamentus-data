

import pandas as pd

import requests
from bs4 import BeautifulSoup

from .page import get_page
from .utils import build_rawdata

def get_assets():
    # response = requests.get('https://www.investsite.com.br/classificacao_setorial_acoes.php')
    # assert(response.status_code == 200)
    # session_id = response.cookies['SESSION_ID']
    # headers = get_assets.headers.copy()
    # headers['cookie'] = f'SESSION_ID={session_id}; _gid=GA1.3.109950653.1696418963; __gads=ID=677dbc9532f65f4d-222656e2a8b4007c:T=1686851767:RT=1696445869:S=ALNI_MalXYE5YMlaeg7ClHEgXeL0Opf7mg; __gpi=UID=00000c4f7016ee69:T=1686851767:RT=1696445869:S=ALNI_MZT9LEQVYq8Jqct4fiIvvnUVw-1-A; _ga=GA1.1.1931325936.1686851770; _ga_LCCM64WERB=GS1.1.1696445022.49.1.1696445904.0.0.0'
    response = requests.get('https://www.investsite.com.br/includes/classificacao_setorial_acoes_source.php', headers=get_assets.headers)
    assert(response.status_code == 200)
    data =  response.json()
    ativos = list()
    for sector in data:
        sector_name = sector['title']
        for subsector in sector['children']:
            subsector_name = subsector['title']
            for segment in subsector['children']:
                segment_name = segment['title']
                for company in segment['children']:
                    company_name = company['title']
                    for asset in company['children']:
                        asset_name = asset['title']
                        asset_name = BeautifulSoup(asset['title'],'html.parser').text
                        ativos.append((sector_name, subsector_name, segment_name, company_name, asset_name))
    return pd.DataFrame(ativos, columns=['setor','subsetor','segmento','empresa','ativo']).set_index('ativo')
    
get_assets.headers = {
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.47',
}