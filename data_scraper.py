#%%
import re
import string
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import tqdm
import yfinance
from bs4 import BeautifulSoup

#%%
dataset_dir = Path('datasets')
def scrap_equity_list():
    pages = ['NUMBER'] + list(string.ascii_uppercase)

    equities = []
    for page in tqdm.tqdm(pages):
        url = 'https://www.set.or.th/set/commonslookup.do'
        params = {'language': 'th', 'country': 'TH', 'prefix': page}
        res = requests.get(url, params=params)

        soup = BeautifulSoup(res.text, features='lxml')

        table_element = soup.find('table', attrs={'class': 'table table-profile table-hover table-set-border-yellow'})
        equity_elements = table_element.find_all('tr')[1:]
        for equity_element in equity_elements:
            symbol, equity_name, market = map(lambda ele: ele.text, equity_element.find_all('td'))
            equities.append({
                'symbol': symbol,
                'equity_name': equity_name,
                'market': market
            })
    equity_df = pd.DataFrame(equities)
    equity_df.to_csv(dataset_dir / 'equity_list.csv',index=False)
# %%
def scrap_equity_description():
    def search_by_keyword(element, keyword):
        pattern = f'{re.escape(keyword)}\s*\n(.+)'
        match = re.search(pattern, element.text, flags=re.MULTILINE)

        value = match.group(1)

        value = re.sub('[,]', '', value).strip()
        if value.replace('.', '').isdigit():
            value = float(value)
        return value
    equity_df = pd.read_csv(dataset_dir / 'equity_list.csv')

    equity_descriptions = []
    for symbol in tqdm.tqdm(equity_df['symbol']):

        url = 'https://www.set.or.th/set/companyprofile.do'
        params = {
            'symbol': symbol,
            'ssoPageId': 4,
            'language': 'en',
            'country': 'US'
        }
        res = requests.get(url, params=params)
        soup = BeautifulSoup(res.text, features='lxml')

        table_element = soup.find('table')

        columns = ('Industry',
            'Sector',
            'Market Cap. (M. Baht)',
        )

        description = pd.Series(data=list(map(lambda col: search_by_keyword(table_element, col), columns)),
                                    index=columns)
        description.rename(index={'Industry': 'industry', 
                                    'Sector': 'sector',
                                    'Market Cap. (M. Baht)': 'market_cap'
        }, inplace=True)
        equity_descriptions.append(description)
    equity_description_df = pd.DataFrame(equity_descriptions)

    equity_df = pd.concat([equity_df, equity_description_df], axis=1)

    equity_df.loc[equity_df['market_cap'] == '-', 'market_cap'] = None
    equity_df['market_cap'] = equity_df['market_cap'].astype(float)

    equity_df.to_csv(dataset_dir / 'thai_equity.csv', index=False)
# %%

def fetch_historical_price():
    equity_df = pd.read_csv(dataset_dir / 'equity_list.csv')

    symbol = equity_df['symbol']

    output_dir = dataset_dir / 'historical_price'
    output_dir.mkdir(exist_ok=True)
    for symbol in tqdm.tqdm(equity_df['symbol']):
        ticker = f"{symbol.replace(' ', '-')}.BK"
        df = yfinance.download(ticker)
        df.to_csv(output_dir / f'{symbol}.csv')

# %%
