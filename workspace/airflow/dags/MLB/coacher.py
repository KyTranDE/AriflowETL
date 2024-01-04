import sys, os
sys.path.append('../')

import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
from pprint import pprint
from datetime import datetime
from config.teams import *
from tqdm.auto import tqdm

HEADERS = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
    'Origin': 'https//www.espn.com',
    'Referer': 'https//www.espn.com/',
    'Sec-Ch-Ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"macOS"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
}
URL_1 = "https://www.espn.com/mlb/managers"
URL_2 = "https://www.baseball-reference.com/bullpen/{COACHER_NAME}"
URL_3 = "https://www.thebaseballcube.com/content/mlb_managers/"

def get_overview():
    response = requests.get(URL_1, headers=HEADERS)
    soup = BeautifulSoup(response.content, 'html.parser')
    tbody = soup.find('table', class_='tablehead')
    trs = tbody.find_all('tr')
    del response, soup, tbody
    data = {
        'name': [],
        'exp': [],
        'record_2023': [],
        'team_name': []
    }
    keys = list(data.keys())
    loop = tqdm(trs[2:], colour='green')
    for tr in loop:
        for key, td in zip(keys, tr.find_all('td')):
            data[key].append(td.text)
        data['team_id'] = data.get('team_id', []) + [TEAMNAME2ID[data['team_name'][-1]]]
        name_coacher = data['name'][-1].replace(' ', '_')
        loop.set_description(f"{name_coacher}")
    df = pd.DataFrame(data)
    df.to_csv('../data/crawl/coacher_crawl.csv', index=False)
    
    
def process():
    df_1 = pd.read_csv('../data/crawl/coacher_crawl.csv')
    df_2 = pd.read_csv('../data/crawl/coacher_download.csv')
    df_2['team_id'] = df_2['team name'].apply(lambda x: TEAMNAME2ID[x])
    df_2.columns = [x.replace(' ', '_') for x in df_2.columns]
    df = pd.merge(df_1, df_2, on='team_id', how='left')
    df = df.rename(columns={'name': 'coacher_name'})
    df = df.replace('--', None)
    df = df[['team_id', 'coacher_name', 'exp', 'record_2023', 'league', 'division', 'since_date', 'tenure', 'age', 'pos', 'ht', 'wt', 'ba', 'th', 'born', 'place', 'hilvl', 'mlb_years', 'stat_years']]
    df.to_csv('../data/process/coacher.csv', index=False)
    
if __name__ == '__main__':
    # get_overview()
    process()