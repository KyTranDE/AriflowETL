import sys 
sys.path.append('..')
from utils.utils import check_requests, write_log, save_json
import pandas as pd
import os 
import json
from tqdm.auto import tqdm


source_path = '../data/crawl/score/'
date_folders = os.listdir(source_path)



for date_folder in tqdm(date_folders):
    folder_path = os.path.join(source_path, date_folder)
    file_names = os.listdir(folder_path)
    for file in file_names:
        if file != 'gameday.json':
            file_path = os.path.join(source_path, date_folder, file)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                try:
                    get_data = {
                        'game_id': data['gamePk'],
                        'year': data['gameData']['datetime']['originalDate'][:4],
                        'date': data['gameData']['datetime']['originalDate'],
                        'host_team': data['gameData']['teams']['home']['name'],
                        'team1': data['gameData']['teams']['away']['name'],
                        'adbreviation_team1': data['gameData']['teams']['away']['abbreviation'],
                        'team2': data['gameData']['teams']['home']['name'],
                        'adbreviation_team2': data['gameData']['teams']['home']['abbreviation'],
                        'team1_score': data['liveData']['linescore']['teams']['away']['runs'], 
                        'team2_score': data['liveData']['linescore']['teams']['home']['runs'], 
                        'win_team': data['gameData']['teams']['away']['name'] if data['liveData']['linescore']['teams']['away']['runs'] > data['liveData']['linescore']['teams']['home']['runs'] else data['gameData']['teams']['home']['name'],
                        'lost_team': data['gameData']['teams']['away']['name'] if data['liveData']['linescore']['teams']['away']['runs'] < data['liveData']['linescore']['teams']['home']['runs'] else data['gameData']['teams']['home']['name'],
                        'winner' : data['liveData']['decisions']['winner']['fullName'] if 'winner' in data['liveData']['decisions'] else None,
                        'loser' : data['liveData']['decisions']['loser']['fullName'] if 'loser' in data['liveData']['decisions'] else None,
                        'save': data['liveData']['decisions']['save']['fullName'] if 'save' in data['liveData']['decisions'] else None
                    }
                    # data_list.append(get_data)
                    write_csv(f'../data/process/history.csv', get_data)
                except:
                    continue
            
# df = pd.DataFrame(data_list)