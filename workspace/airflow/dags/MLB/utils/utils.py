import os
import json
import pandas as pd
import datetime
import requests
import csv

import sys
sys.path.append('../')
# from configs.params import START_DATE, END_DATE


def get_timeline(start_date: str, end_date: str, format_day = '%Y-%m-%d'):
    return [day.strftime(format_day) for day in pd.date_range(start_date, end_date)]

def get_year(year):
    year_now = datetime.datetime.now().year
    if year_now <= year:
        return int(datetime.datetime.now().strftime('%Y'))
    return int(year)

def get_day(year=2023, format_day = '%Y-%m-%d'):
    y_now = datetime.datetime.now().year
    if year >= y_now:
        return get_timeline(f'{y_now}-01-01', END_DATE, format_day)
    return get_timeline(f'{year}-01-01', f'{year}-12-31', format_day)

def check_requests(url, HEADERS=None):
    check_out = 3
    res = None
    while check_out > 0:
        res = requests.get(url, headers=HEADERS)
        if res.status_code == 200: 
            return res
        check_out -= 1
    return None
        
def process_data(data, percent=False):
    if data is None:
        return None
    else:
        if isinstance(data, str):
            return data.strip()
        else:
            if percent:
                return round(data*100, 3)
            else:
                return round(data, 3)
            
def collect_data(data, key):
    res = data.copy()
    if res is None:
        return None
    keys = key.split('.')
    try:
        for key in keys:
            res = res.get(key)
        return None if res in ('.---', '-.--') else res
    except:
        return None

# --- Anotation ---
def write_log(file, text='', mode ='a'):
    os.makedirs(f'{"/".join(file.split("/")[:-1])}', exist_ok=True)
    with open(f'{file}', mode, encoding='utf-8') as f:
        f.write(str(text) + '\n')
        f.close()
        
def write_csv(path: str, data: dict, mode = 'a') -> None:
    '''
    write data to csv file
    '''
    # check path, if not exist then create
    os.makedirs(f'{"/".join(path.split("/")[:-1])}', exist_ok=True)
    with open(f'{path}', mode, newline='') as f:
        writer = csv.writer(f)
        # check data in csv file, if empty then write header
        if os.stat(f'{path}').st_size == 0:
            writer.writerow(data.keys())
        writer.writerow(data.values())
        f.close()

def save_json(data, path, mode='a'):
    os.makedirs(f'{"/".join(path.split("/")[:-1])}', exist_ok=True)
    with open(path, mode, encoding='utf-8') as f:
        # load data from json file
        if os.stat(f'{path}').st_size == 0:
            json.dump(data, f, ensure_ascii=False, indent=4)
        else:
            data_old = json.load(f)
            data_old.update(data)
            json.dump(data_old, f, ensure_ascii=False, indent=4)
        f.close()

def get_folder_size(folder_path):
    total_size = 0
    for path, dirs, files in os.walk(folder_path):
        for f in files:
            file_path = os.path.join(path, f)
            total_size += os.path.getsize(file_path)
    size_in_kb = total_size / 1024
    size_in_mb = size_in_kb / 1024
    size_in_gb = size_in_mb / 1024
    print({'KB': size_in_kb, 'MB': size_in_mb, 'GB': size_in_gb})
    
