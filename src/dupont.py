import concurrent.futures as cf
import pandas as pd

import requests
from bs4 import BeautifulSoup

from .page import get_page
from .utils import build_rawdata

def get_dupont(asset):
    _, global_vars = get_page(asset)
    data = build_rawdata({
        'cod_negociacao',
        'tipoemp',
        'codcvm',
    },
        global_vars
    )
    data += '&ano_inicio=2000'
    data += '&mes_inicio=0'
    headers = get_dupont.headers.copy()
    headers['cookie'] = f'SESSION_ID={global_vars["session_id"]}; _gid=GA1.3.109950653.1696418963; __gads=ID=677dbc9532f65f4d-222656e2a8b4007c:T=1686851767:RT=1696445869:S=ALNI_MalXYE5YMlaeg7ClHEgXeL0Opf7mg; __gpi=UID=00000c4f7016ee69:T=1686851767:RT=1696445869:S=ALNI_MZT9LEQVYq8Jqct4fiIvvnUVw-1-A; _ga=GA1.1.1931325936.1686851770; _ga_LCCM64WERB=GS1.1.1696445022.49.1.1696445904.0.0.0'
    headers['csrf-token'] = global_vars['token']
    headers['referer'] = f'https://www.investsite.com.br/principais_indicadores.php?cod_negociacao={asset}'
    graphics = [
        'RETORNO_PL_ANUAL',
        'RETORNO_ATIVO_ANUAL',
        'MARGEM_LIQUIDA_ANUAL',
        'ALAVANCAGEM_FINANCEIRA_ANUAL',
        'GIRO_ATIVO_ANUAL',
    ]
    dupont = dict()
    num_threads = len(graphics)
    with cf.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(get_dupont_request, headers=headers, data=data, tipo_grafico=tipo_grafico) for tipo_grafico in graphics]
        for future in cf.as_completed(futures):
            name, dupont_data = future.result()
            dupont[name] = pd.Series(dupont_data.dadosgrafico, index=pd.to_datetime(dupont_data.datas_data_tip.str.split().str.get(0), format='%m/%Y'), name=name)
    df_dupont = pd.DataFrame()
    for d in dupont.values():
        df_dupont = df_dupont.join(d, how='outer')
    # complete missing data
    na_idx = df_dupont.ALAVANCAGEM_FINANCEIRA_ANUAL.isna()
    df_dupont['ALAVANCAGEM_FINANCEIRA_ANUAL'] = df_dupont.loc[na_idx,'RETORNO_PL_ANUAL']/df_dupont.loc[na_idx,'RETORNO_ATIVO_ANUAL']
    df_dupont.ffill(inplace=True)
    return df_dupont

def get_dupont_request(headers, data, tipo_grafico):
    data += '&tipo_grafico='+tipo_grafico
    response = requests.post('https://www.investsite.com.br/includes/graficos_desempenho_operacional_ajax.php', headers=headers, data=data)
    assert(response.status_code == 200)
    dupont = pd.DataFrame(response.json())
    dupont['dadosgrafico'] = dupont['dadosgrafico'].astype(float)
    dupont.index = pd.to_datetime(dupont.datas_data_tip.str.split().str.get(0), format='%m/%Y')
    return tipo_grafico, dupont

get_dupont.headers = {
    'authority': 'www.investsite.com.br',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://www.investsite.com.br',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.47',
}