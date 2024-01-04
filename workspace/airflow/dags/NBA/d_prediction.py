from NBA.config import Config
import pandas as pd
import pymysql
import numpy as np
import math
import joblib

import warnings
import datetime
import re


warnings.filterwarnings('ignore')

database=Config.database

mydb = pymysql.connect(
    host=Config.host,
    port=Config.port,
    user=Config.user,
    password=Config.password,
    database=Config.database
)

team_names = [
    "Boston Celtics",
    "Brooklyn Nets",
    "New York Knicks",
    "Philadelphia 76ers",
    "Toronto Raptors",
    "Chicago Bulls",
    "Cleveland Cavaliers",
    "Detroit Pistons",
    "Indiana Pacers",
    "Milwaukee Bucks",
    "Atlanta Hawks",
    "Charlotte Hornets",
    "Miami Heat",
    "Orlando Magic",
    "Washington Wizards",
    "Denver Nuggets",
    "Minnesota Timberwolves",
    "Oklahoma City Thunder",
    "Portland Trail Blazers",
    "Utah Jazz",
    "Golden State Warriors",
    "LA Clippers",
    "Los Angeles Lakers",
    "Phoenix Suns",
    "Sacramento Kings",
    "Dallas Mavericks",
    "Houston Rockets",
    "Memphis Grizzlies",
    "New Orleans Pelicans",
    "San Antonio Spurs"
]


list_tables_train = ['BoxScores_Traditional','BoxScores_Defense','BoxScores_Fourfactors','BoxScores_Hustle','BoxScores_Misc','BoxScores_Scoring','BoxScores_Tracking','BoxScores_Advanced','BoxScores_Usage']

column_basic=['team_name','player_name']

column_traditional=['fgm', 'fga', '3pm', '3pa', '3p_percent', 'ftm', 'fta', 'ft_percent', 'oreb', 'dreb', 'reb', 'pts']

column_defense=['def_min','partial_poss','pts','dreb', 'ast','dfgm', 'dfga','d3pa', 'd3p_percent']

column_fourfactors=['fta_rate', 'tm_to_percent', 'opp_efg_percent', 'opp_fta_rate', 'opp_to_percent','opp_oreb_percent']

column_hustle= ['screen_ast', 'screen_ast_pts', 'off_loose_balls_recovered', 'def_loose_balls_recovered', 'loose_balls_recovered', 'contested_2pt_shots', 'contested_3pt_shots', 'contested_shots', 'off_box_outs', 'def_box_outs', 'box_outs']

column_misc=['min','opp_pitp']

column_scoring=['percent_fga_2pt','percent_fga_3pt', 'percent_pts_2pt','percent_pts_3pt','percent_pts_pitp','2fgm_percent_ast','2fgm_percent_uast','3fgm_percent_ast','fgm_percent_ast', 'fgm_percent_uast']

column_tracking=['spd', 'dist', 'orbc','drbc', 'rbc', 'tchs', 'pass', 'ast', 'cfgm', 'cfga', 'cfg_percent', 'ufgm', 'ufga', 'ufg_percent', 'fg_percent', 'dfgm', 'dfga', 'dfg_percent']


column_advanced=['offrtg','defrtg','netrtg','ast_percent','ast_ratio','oreb_percent','dreb_percent','reb_percent','efg_percent','ts_percent']

column_usage=['usg_percent', 'percent_fgm', 'percent_fga', 'percent_3pm', 'percent_3pa', 'percent_ftm', 'percent_fta', 'percent_dreb', 'percent_reb', 'percent_pfd', 'percent_pts']

sort_col=['team_1', 'team_2', 'team_name', 'player_name', 'offrtg', 'defrtg',
        'netrtg', 'ast_percent', 'ast_ratio', 'oreb_percent',
        'dreb_percent', 'reb_percent', 'efg_percent', 'ts_percent',
        'def_min', 'partial_poss', 'def_pts', 'def_dreb', 'def_ast',
        'def_dfgm', 'def_fga', 'def_3pa', 'd3p_percent', 'fta_rate',
        'tm_to_percent', 'opp_efg_percent', 'opp_fta_rate',
        'opp_to_percent', 'opp_oreb_percent', 'screen_ast',
        'screen_ast_pts', 'off_loose_balls_recovered',
        'def_loose_balls_recovered', 'loose_balls_recovered',
        'contested_2pt_shots', 'contested_3pt_shots', 'contested_shots',
        'off_box_outs', 'def_box_outs', 'box_outs', 'min', 'opp_pitp',
        'percent_fga_2pt', 'percent_fga_3pt', 'percent_pts_2pt',
        'percent_pts_3pt', 'percent_pts_pitp', '2fgm_percent_ast',
        '2fgm_percent_uast', '3fgm_percent_ast', 'fgm_percent_ast',
        'fgm_percent_uast', 'spd', 'dist', 'orbc', 'drbc', 'rbc', 'tchs',
        'pass', 'ast', 'cfgm', 'cfga', 'cfg_percent', 'ufgm', 'ufga',
        'ufg_percent', 'fg_percent', 'dfgm', 'dfga', 'dfg_percent',
        'usg_percent', 'percent_fgm', 'percent_fga', 'percent_3pm',
        'percent_3pa', 'percent_ftm', 'percent_fta', 'percent_dreb',
        'percent_reb', 'percent_pfd', 'percent_pts', 'fgm', 'fga', '3pm',
        '3pa', '3p_percent', 'ftm', 'fta', 'ft_percent', 'oreb', 'dreb',
        'reb']

def get_column(df_name):
    if df_name == 'advanced':
        return column_basic+column_advanced
    elif df_name == 'misc':
        return column_basic+column_misc
    elif df_name == 'defense':
        return column_basic+column_defense
    elif df_name == 'scoring':
        return column_basic+column_scoring
    elif df_name == 'fourfactors':
        return column_basic+column_fourfactors
    elif df_name == 'tracking':
        return column_basic+column_tracking
    elif df_name == 'hustle':
        return column_basic+column_hustle
    elif df_name == 'traditional':
        return column_basic+column_traditional
    elif df_name == 'usage':
        return column_basic+column_usage
    
def convert_to_seconds(time_str):
    if 'PT' in time_str:
        match = re.search(r'(\d+)', time_str)
        hours = 0
        minutes = int(match.group()) if match else 0
        seconds = 0
        if 'M' in time_str:
            match = re.search(r'M(\d+)', time_str)
            seconds = int(match.group(1)) if match else 0
    elif ':' in time_str:
        parts = time_str.split(':')
        if len(parts) == 2:
            hours = 0
            minutes = int(parts[0])
            seconds = int(parts[1])
        elif len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2])
    else:
        hours = 0
        minutes = int(time_str)
        seconds = 0
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

def df_convert_min(dataframe):
    name_column=""
    try:
        columns = dataframe.columns
        if 'min' in columns:
            name_column = 'min'
        elif "def_min" in columns:
            name_column = 'def_min'
        if 'Periods' in columns:
            del dataframe['Periods']
        if 'Position' in columns:
            del dataframe['Position']
        if name_column:
            dataframe[name_column]=dataframe[name_column].apply(convert_to_seconds)
            dataframe[name_column] = dataframe[name_column].astype(int)
            dataframe=dataframe.round(2)
    except Exception as e:
        columns = dataframe.columns
        if 'min' in columns:
            name_column = 'min'
        elif "def_min" in columns:
            name_column = 'def_min'
        dataframe = dataframe.copy()
        if 'Periods' in columns:
            del dataframe['Periods']
        if 'Position' in columns:
            del dataframe['Position']
        if name_column:
            dataframe[name_column]=dataframe[name_column].apply(convert_to_seconds)
        dataframe=dataframe.round(2)
    return dataframe

def mean_sum_df(df):
    data_merge=df.groupby(['team_name', 'player_name']).mean().reset_index()
    data_merge=data_merge.round(2)
    return data_merge

def convert_day(x):
    list_x = x.split('-')
    if len(list_x[0])==4:
        return x
    else:
        x=list_x[2]+'-'+list_x[1]+'-'+list_x[0]
        return x
    
def df_team_num_lasted(name_team, tables, num_lasted=5,database=database):
    text_sql = f"SELECT * FROM {database}.{tables}  where team_name = '{name_team}'"
    df= pd.read_sql_query(text_sql, mydb, index_col='id')
    # print(df)
    df['day'] = df['day'].apply(convert_day)
    df = df.sort_values(by='day', ascending=False)
    unique_day = df['day'].unique()
    ten_day_lasted= unique_day[:num_lasted]
    df = df[df['day'].isin(ten_day_lasted)]
    name_table=tables.split('_')[1].lower()
    name_col= get_column(name_table)
    df = df[name_col]
    df = df_convert_min(df)
    if name_table == 'defense':
        df.rename(columns={'pts':'def_pts','dreb':'def_dreb','ast':'def_ast','dfgm':'def_dfgm','dfga':'def_fga','d3pa':'def_3pa','d3p_percent':'d3p_percent'}, inplace=True)
    df = mean_sum_df(df)
    return df

def get_info_player_team(list_df,team_name):
    list_player=[]
    for key,val in list_df.items():
        list_player.extend(val[val['team_name']==team_name]['player_name'].unique())
    list_player=list(set(list_player))

    df_total_merge=pd.DataFrame()
    for player_name in list_player:
        df_mer=pd.DataFrame()
        for i,(key,val) in enumerate(list_df.items()):
            if i == 0:
                df_mer=val[val['player_name']==player_name]
            else:
                df_mer=pd.merge(df_mer, val[val['player_name']==player_name], on=['team_name','player_name'], how='left')
        df_total_merge=pd.concat([df_total_merge,df_mer])
    return df_total_merge.reset_index(drop=True)

def get_info_team(team_name, list_tables_train, num_lasted=10):
    list_df={}
    for table in list_tables_train:
        df=df_team_num_lasted(team_name, table, num_lasted)
        list_df[table]=df.reset_index(drop=True)
    
    df_total_merge = get_info_player_team(list_df,team_name)
    return df_total_merge

def label_encoder_scale(df, la_name_team,la_name_player,scale):
    for i in df.select_dtypes(include=['object']).columns:
        if 'eam' in i:
            df[i]=la_name_team.transform(df[i])
        else:
            # la_name_player=check_player_name_label(df[i], la_name_player)
            df[i]=la_name_player.transform(df[i])
    df=scale.transform(df)
    return df

def scale_score(score):
    if score >= 130:
        chenh_lech = score - 120
        chenh_lech_moi = 130 - 100
        tyle = chenh_lech_moi / chenh_lech
        score = 100 + (score - 130) * tyle
        return math.floor(score)
    else:
        return math.floor(score)
    
def prediction_score(model,df_betrates):
    df_betrates[np.isnan(df_betrates)] = 0
    outputs = model.predict(df_betrates)
    predicted_labels=np.array(outputs)
    predicted_labels[np.isnan(predicted_labels)] = 0
    total_score=sum(predicted_labels)
    total_score_scale=scale_score(total_score)
    return total_score_scale

def prediction(model,team1,team2,scale,la_name_team,la_name_player,list_tables_train=list_tables_train,sort_col=sort_col,number_last=10):
    df_total_merge_team1 = get_info_team(team1, list_tables_train,number_last)
    df_total_merge_team2 = get_info_team(team2, list_tables_train,number_last)
    # return df_total_merge_team2
    data_predict={
        'team_1': team1,
        'team_2': team2,
        }
    df_betrates = pd.DataFrame(data_predict, index=[0])
    # return df_betrates_1,df_betrates_2
    df_betrates_1=df_betrates.merge(df_total_merge_team1, left_on='team_1', right_on='team_name', how='left')[sort_col]
    df_betrates_2=df_betrates.merge(df_total_merge_team2, left_on='team_2', right_on='team_name', how='left')[sort_col]
    df_betrates_1=label_encoder_scale(df_betrates_1,la_name_team,la_name_player,scale)
    df_betrates_2=label_encoder_scale(df_betrates_2,la_name_team,la_name_player,scale)
    # return df_betrates,df_betrates_1,df_betrates_2
    df_betrates['Team_1_Score_predict']=prediction_score(model,df_betrates_1)
    df_betrates['Team_2_Score_predict']=prediction_score(model,df_betrates_2)
    df_betrates['Total_Predict_Score']=df_betrates['Team_1_Score_predict']+df_betrates['Team_2_Score_predict']
    return df_betrates

def get_prediction(team1,team2,folder_save):
    la_name_player = joblib.load(f'{folder_save}/la_player_new.pkl')
    la_name_team = joblib.load(f'{folder_save}/la_team_new.pkl')
    scale= joblib.load(f'{folder_save}/scaler.pkl')
    model=joblib.load(f'{folder_save}/model.pkl')
    try:
        df_betrates=prediction(model,team1,team2,scale=scale,la_name_team=la_name_team,la_name_player=la_name_player,number_last=10)
    except:
        print('error')
        df_betrates=pd.DataFrame({
            'Team_1': team1,
            'Team_2': team2,
            'Team_1_Score_predict': 0,
            'Team_2_Score_predict': 0,
            'Total_Score_predict': 0,
        })
    return df_betrates