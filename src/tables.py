import requests
from bs4 import BeautifulSoup
import concurrent.futures as cf
import logging
import pandas as pd

from .page import get_page
from .utils import build_rawdata

def get_tables(asset):
    page, global_vars = get_page(asset)
    tables = {}
    # get static tables
    static_table_pages = page.select(get_tables.CSS_SELECTOR_STATIC_TABLES)
    for table in static_table_pages:
        table_name = table.select_one('caption').text
        logging.debug(table_name)
        current_table = tables[table_name] = {}
        for name, value in zip(table.select('.esquerda'), table.select('.direita')):
            current_table[name.text] = value.text
        tables[table_name] = pd.Series(current_table)
    # get dynamic tables
    dynamic_table_pages = page.select(get_tables.CSS_SELECTOR_DYNAMIC_TABLES)
    num_threads = len(dynamic_table_pages)
    with cf.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(request_dynamic_table_data, table_page, **global_vars)
            for table_page in dynamic_table_pages
        ]
        for future in cf.as_completed(futures):
            table_name, table = future.result()
            logging.debug(table_name)
            tables[table_name] = table
    return pd.Series(tables)

get_tables.CSS_SELECTOR_STATIC_TABLES = 'table:not(:has(> thead)):has(> caption)'
get_tables.CSS_SELECTOR_DYNAMIC_TABLES = 'table:has(> thead)'

def request_dynamic_table_data(page, **kwargs):
    dynamic_table_attrs = [{**el.attrs, 'date': el.text} for el in page.select(f'select.data_tabela option')]
    table_name = page.select_one('caption').text
    kwargs['tabela_id'] = page['id']
    tables = {}
    num_threads = len(dynamic_table_attrs)
    with cf.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(request_dynamic_table_single_data, **attr, **kwargs) for attr in dynamic_table_attrs]
        for future in cf.as_completed(futures):
            args, table = future.result()
            date = args['date']
            logging.debug(table_name, date)
            tables[date] = table
    tables = pd.DataFrame(tables).T
    tables.index = pd.to_datetime(tables.index.str.replace("Atual - ", ""), dayfirst=True)
    return table_name, tables

def request_dynamic_table_single_data(**kwargs):
    kwargs['tipodata'] = kwargs['data-tipodata']
    kwargs['versao'] = kwargs['data-versao']
    kwargs['data_tabela'] = kwargs['value']
    kwargs['cod_negociacao'] = kwargs['asset']
    kwargs['tipo_ord_pref'] = kwargs['assert_type']
    data = build_rawdata({
            'tipodata',
            'versao',
            'data_tabela',
            'cons_tabela',
            'codcvm',
            'cod_negociacao',
            'tabela_id',
            'tipo_ord_pref',
            'tipoemp',
        },
        kwargs
    )
    headers = request_dynamic_table_single_data.headers.copy()
    headers['cookie'] = f'SESSION_ID={kwargs["session_id"]}; _gid=GA1.3.109950653.1696418963; __gads=ID=677dbc9532f65f4d-222656e2a8b4007c:T=1686851767:RT=1696445869:S=ALNI_MalXYE5YMlaeg7ClHEgXeL0Opf7mg; __gpi=UID=00000c4f7016ee69:T=1686851767:RT=1696445869:S=ALNI_MZT9LEQVYq8Jqct4fiIvvnUVw-1-A; _ga=GA1.1.1931325936.1686851770; _ga_LCCM64WERB=GS1.1.1696445022.49.1.1696445904.0.0.0'
    headers['csrf-token'] = kwargs['token']
    headers['referer'] = f'https://www.investsite.com.br/principais_indicadores.php?cod_negociacao={kwargs["asset"]}'
    resp = requests.post('https://www.investsite.com.br/includes/principais_indicadores_ajax.php',headers=headers,data=data)
    assert(resp.status_code == 200)
    page = BeautifulSoup(resp.text, 'html.parser')
    table = {}
    for k, v in zip(page.select('.esquerda'), page.select('.direita')):
        table[k.text] = v.text
    return kwargs, table

request_dynamic_table_single_data.headers = {
        'authority': 'www.investsite.com.br',
        'accept': '*/*',
        'accept-language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://www.investsite.com.br',
        'sec-ch-ua': '"Microsoft Edge";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.47',
        'x-requested-with': 'XMLHttpRequest',
    }

def test_func(page, **kwargs):
    table_name = page.select_one('caption').text
    logging.debug(table_name, kwargs)
    return table_name, {}