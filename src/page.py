import requests
from bs4 import BeautifulSoup
import logging

def get_page(asset):
    url = f"https://www.investsite.com.br/principais_indicadores.php?cod_negociacao={asset}"
    logging.debug('requesting page:',asset)
    response = requests.get(url,headers=get_page.headers)
    assert(response.status_code == 200)
    page = BeautifulSoup(response.text, 'html.parser')
    global_vars = dict()
    global_vars['assert_type'] = page.select_one('input[name="tipo_ord_pref"]')['value']
    global_vars['token'] = page.select_one('meta[name="csrf-token"]')['content']
    global_vars['session_id'] = response.cookies['SESSION_ID']
    global_vars['asset'] = asset
    global_vars['cons_tabela'] = 2
    for var in page.select_one('script[type="text/javascript"]').text.replace('var','').split(';'):
        if var:
            k,v = map(lambda d: d.strip(), var.split('='))
            global_vars[k] = eval(v)
    return page, global_vars

get_page.headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.47',
}