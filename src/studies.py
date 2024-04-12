from .tables import get_tables
from .price import get_prices
import pandas as pd

def calc_lpa(table, prices):
    if 'Lucro/Ação' in table['Resumo DRE Últimos Doze Meses'].columns:
        lpa = table['Resumo DRE Últimos Doze Meses']['Lucro/Ação'] \
            .str.replace('-?\s*R\$','',regex=True) \
            .str.replace(',','.',regex=False) \
            .str.replace('NA','NAN') \
            .astype(float) \
            .sort_index()
    else:
        ll = table['Resumo DRE Últimos Doze Meses']['Lucro Líquido'].sort_index().apply(lambda l: eval(l.replace('R$','').replace('T','*10**3B').replace('B','*10**3M').replace('M','*10**6').replace(' ','').replace('.','').replace(',','.').replace('NA','None')))
        na = table['Resumo Balanço Patrimonial']['Total'].sort_index().apply(lambda l: eval(l.replace('R$','').replace('T','*10**3B').replace('B','*10**3M').replace('M','*10**6').replace(' ','').replace('.','').replace(',','.').replace('NA','None')))
        lpa = ll/na
    lpa.dropna(inplace=True)
    lpa_full = pd.Series(index=prices.index.union(lpa.index), dtype=float)
    lpa_full.loc[lpa.index] = lpa
    lpa_full.ffill(inplace=True)
    lpa_full = lpa_full[prices.index]
    lpa_full.name='lpa'
    return prices.join(lpa_full).dropna()

def calc_pl(table, prices):
    lpa = calc_lpa(table, prices)
    return lpa.drop(['lpa','volumes'],axis=1).apply(lambda d: d/lpa['lpa'], axis=0)

def get_lpa(asset):
    table = get_tables(asset,'(Resumo DRE Últimos Doze Meses|Resumo Balanço Patrimonial)')
    prices = get_prices(asset)
    return calc_lpa(table, prices)

def get_pl(asset):
    table = get_tables(asset,'(Resumo DRE Últimos Doze Meses|Resumo Balanço Patrimonial)')
    prices = get_prices(asset)
    return calc_pl(table, prices)
