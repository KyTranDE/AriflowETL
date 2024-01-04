def bet_rate():
    import requests
    import pandas as pd
    from tqdm import tqdm
    tqdm.pandas(desc="progress")
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock
    # import sys
    # sys.path.append('../')
    # from utils.utils import collect_data
    from MLB.config.teams import CODE2ID_BETRATE
    headers = {
        'authority': 'api.nflpickwatch.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en,vi;q=0.9',
        'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MTEwNjY0LCJjbGFpbXMiOlsicCJdLCJpYXQiOjE2OTU2MzExNzEsImV4cCI6MTcxMTQ0MjM3MX0.6mpM8uRxTwwWOkQDCEF51eMkvEe34hfazP3YsUWKnYM',
        'origin': 'https://nflpickwatch.com',
        'referer': 'https://nflpickwatch.com/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    }

    file_lock = Lock()

    def get_data(url):
        try:
            response = requests.get(url, headers=headers)
            tagter = response.json()
            for i in tagter:
                data_moneyline = {}
                data_moneyline['game_date'] = i['additional_data']['Day'].split('T')[0]
                data_moneyline['home_team_id'] = CODE2ID_BETRATE.get(i['home_team_id'],i['home_team_id'])
                data_moneyline['away_team_id'] = CODE2ID_BETRATE.get(i['road_team_id'],i['road_team_id'])
                data_moneyline['home_team_consensus'] = i['ht_pct_su_combined']
                data_moneyline['away_team_consensus'] = i['rt_pct_su_combined']
                with file_lock:
                    df = pd.DataFrame([data_moneyline])
                    df.to_csv('./data/moneyline.csv', mode='a', header=not pd.io.common.file_exists('./data/moneyline.csv'), index=False)
                    
        
            for i in tagter:
                data_runline = {}
                data_runline['game_date'] = i['additional_data']['Day'].split('T')[0]
                data_runline['home_team_id'] = CODE2ID_BETRATE.get(i['home_team_id'],i['home_team_id'])
                data_runline['away_team_id'] = CODE2ID_BETRATE.get(i['road_team_id'],i['road_team_id'])
                data_runline['home_team_line'] = i['home_team_spread']
                data_runline['away_team_line'] = i['road_team_spread']
                data_runline['home_team_consensus'] = i['ht_pct_ats_combined']
                data_runline['away_team_consensus'] = i['rt_pct_ats_combined']
                
                with file_lock:
                    df = pd.DataFrame([data_runline])
                    df.to_csv('./data/run_line.csv', mode='a', header=not pd.io.common.file_exists('./data/run_line.csv'), index=False)
                    
            for i in tagter:
                data_pointstotal = {}
                data_pointstotal['game_date'] = i['additional_data']['Day'].split('T')[0]
                data_pointstotal['home_team_id'] = CODE2ID_BETRATE.get(i['home_team_id'],i['home_team_id'])
                data_pointstotal['away_team_id'] = CODE2ID_BETRATE.get(i['road_team_id'],i['road_team_id'])
                data_pointstotal['over_under'] = i['over_under']
                data_pointstotal['home_team_consensus'] = i['ht_pct_ou_combined']
                data_pointstotal['away_team_consensus'] = i['rt_pct_ou_combined']
                with file_lock:
                    df = pd.DataFrame([data_pointstotal])
                    df.to_csv('./data/points_total.csv', mode='a', header=not pd.io.common.file_exists('./data/points_total.csv'), index=False)
        except Exception as e:
            print(e)
            pass

    # start_url = "https://api.nflpickwatch.com/v1/general/games/2022/114/mlb" # 210 # 211
    def crawl():
        list_url = []
        for i in range(2021, 2024):
            for _ in range(300):
                list_url.append(f"https://api.nflpickwatch.com/v1/general/games/{i}/{_+1}/mlb")
                
            with ThreadPoolExecutor(max_workers=128) as executor:
                executor.map(get_data, list_url)


    # # Path: services/bet_rate.py
    def merge_data(df_game, df):
        df_merge = pd.merge(df, df_game[['game_id', 'home_team_id', 'away_team_id', 'game_date']], on=['home_team_id', 'away_team_id', 'game_date'], how='left')
        df_merge.dropna(subset=['game_id'], inplace=True)
        df_merge['game_id'] = df_merge['game_id'].astype(int)
        df_merge.drop_duplicates(subset=['game_id'], inplace=True)
        return df_merge
        
    def process():
        df_game = pd.read_csv('./data/game.csv')
        df_ml = pd.read_csv('./data/moneyline.csv')
        df_rl = pd.read_csv('./data/run_line.csv')
        df_pt = pd.read_csv('./data/points_total.csv')
        
        for df, name in zip([df_ml, df_rl, df_pt], ['moneyline', 'run_line', 'points_total']):
            df_merge = merge_data(df_game, df)
            df_merge.to_csv(f'./data/{name}.csv', index=False)

    if __name__ == '__main__':
        crawl()
        process()