from glob import glob
import json
from threading import Thread
from tqdm import tqdm
import pandas as pd
import concurrent.futures
from threading import Lock
write_lock = Lock()

import sys
sys.path.append('..')
from utils.utils import check_requests, write_log, save_json, write_csv, collect_data
from utils.logging import logger

url_home = 'https://statsapi.mlb.com/api/v1/sports/1/players?fields=people,fullName,lastName,nameSlug&season={season}'
url_player = 'https://statsapi.mlb.com/api/v1/people/{id}?hydrate=currentTeam,team,stats(type=[yearByYear,yearByYearAdvanced,careerRegularSeason,careerAdvanced,availableStats](team(league)),leagueListId=mlb_hist)&site=en'
url_id = 'https://statsapi.mlb.com/api/v1/people/{id}'
mapping_table = str.maketrans({'"':'', "'":'', ' ':'.'})

def get_players(season):
    response = check_requests(url_home.format(season=season))
    if not response:
        return None
    return response.json()['people']

def get_info_player(id):
    response = check_requests(url_player.format(id=id))
    if not response:
        return None
    return response.json()['people'][0]
    
def crawl(season):
    lst_player = [id['nameSlug'].split('-')[-1] for id in get_players(season)]
    print(len(lst_player))
    for id in lst_player:
        info = get_info_player(id)
        status = f'player {id} in season {season}'
        if info:
            save_json(info, f'../data/crawl/player/{season}/{id}.json')
            logger('info', f'Successfully crawl player_id {id} in season {season}')
        else:
            status = f'[Error] {status}'
            logger('error', f'Error when crawl player_id {id} in season {season}')
        write_log('../log/player.log', status)

def process(season):
    seasons = glob(f'../data/crawl/player/{season}/*.json')
    data = {
        'player_id': 'id', # id: mã số người chơi
        'player_name': 'fullName', # fullName: tên đầy đủ
        'gender': 'gender', #gender: giới tính
        'nick_name': 'nickName', # useName: tên người dùng
        'position': 'primaryPosition.abbreviation', # primaryPosition.abbreviation: vị trí chính
        'age': 'currentAge', # currentAge: tuổi
        'birth_day': 'birthDate', # birthDate: ngày sinh
        'birth_city': 'birthCity', # birthCity: thành phố nơi sinh
        'birth_country': 'birthCountry', # birthCountry: quốc gia nơi sinh
        'height': 'height', # height: chiều cao
        'weight': 'weight', # weight: cân nặng
        'debut': 'mlbDebutDate', # mlbDebutDate: ngày bắt đầu chơi
        'last_played_date': 'lastPlayedDate', # lastPlayedDate: ngày chơi cuối cùng
        'bat_side': 'batSide.code', # batSide.code: cầm gậy tay nào
        'pitch_hand': 'pitchHand.code' # pitchHand.code: ném bóng tay nào
        
    }
    loop = tqdm(seasons)
    for s in loop:
        temp = {}
        with open(s, 'r+', encoding='utf-8') as f:
            json_data = json.load(f)
            temp['season'] = season#s.split('/')[-2]
            for key in data:
                temp[key] = collect_data(json_data, data[key])
            try:
                temp['height'] = float(str(temp['height']).translate(mapping_table))
            except:
                continue
            loop.set_description(f'{temp["season"]}')
            loop.set_postfix(name = temp['player_name'])
            write_csv(f'../data/process/player.csv', temp)
    
def fetch_id(player_id):
    data = {
        'player_id': 'id', # id: mã số người chơi
        'player_name': 'fullName', # fullName: tên đầy đủ
        'gender': 'gender', #gender: giới tính
        'nick_name': 'nickName', # useName: tên người dùng
        'position': 'primaryPosition.abbreviation', # primaryPosition.abbreviation: vị trí chính
        'age': 'currentAge', # currentAge: tuổi
        'birth_day': 'birthDate', # birthDate: ngày sinh
        'birth_city': 'birthCity', # birthCity: thành phố nơi sinh
        'birth_country': 'birthCountry', # birthCountry: quốc gia nơi sinh
        'height': 'height', # height: chiều cao
        'weight': 'weight', # weight: cân nặng
        'debut': 'mlbDebutDate', # mlbDebutDate: ngày bắt đầu chơi
        'last_played_date': 'lastPlayedDate', # lastPlayedDate: ngày chơi cuối cùng
        'bat_side': 'batSide.code', # batSide.code: cầm gậy tay nào
        'pitch_hand': 'pitchHand.code' # pitchHand.code: ném bóng tay nào
    }
    # list_id = []
    # with open('../data/process/player_id.txt', 'r+', encoding='utf-8') as f:
    #     for line in f.readlines():
    #         list_id.append(line.split('\n')[0])
    # loop = tqdm(list_id, desc='process player')
    # for s in loop:
    temp = {}
    req = check_requests(url_id.format(id=player_id))
    json_data = req.json()['people'][0]
    for key in data:
        temp[key] = collect_data(json_data, data[key])
    try:
        temp['height'] = float(str(temp['height']).translate(mapping_table))
    except:
        # continue
        pass
    # loop.set_postfix(player_id = temp['player_id'])
    
    with write_lock:
        # write_csv(f'../data/process/player_100.csv', temp)
        df = pd.DataFrame([temp])
        df.to_csv('../data/process/player.csv', mode='a', header=not pd.io.common.file_exists('../data/process/player.csv'), index=False, encoding='utf-8')
        
            
def run(start_season=2013, end_season=2023):
    try:
        for s in range(start_season, end_season+1):
            t = Thread(target=crawl, args=(s,))
            t.start()
    except:
        print ("error")
    
if __name__ == '__main__':
    # for i in range(2013, 2024):
    #     process(i)
    list_id = []
    # with open('../data/process/player_id.txt', 'r+', encoding='utf-8') as f:
    #     for line in f.readlines():
    #         list_id.append(line.split('\n')[0])
    # --- option 2 ---
    list_player = ['boxscore_batting_player', 'boxscore_fielding_player', 'boxscore_pitching_player']

    df_1 = pd.read_csv(f'../data/process/{list_player[0]}.csv')
    df_2 = pd.read_csv(f'../data/process/{list_player[1]}.csv')
    df_3 = pd.read_csv(f'../data/process/{list_player[2]}.csv')

    df = pd.concat([df_1[['player_id', 'team_id']], df_2[['player_id', 'team_id']], df_3[['player_id', 'team_id']]], axis=0).reset_index(drop=True)
    df.drop_duplicates(subset='player_id', inplace=True)
    for iter in df.iterrows():
        list_id.append(iter[1][0])
    # --- end ---
    num_thread = 100
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_thread) as executor:
        executor.map(fetch_id, list_id)

## 1.37 GB data json 