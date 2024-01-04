from NBA.config import Config
from sqlalchemy import create_engine
import pandas as pd
from urllib.parse import quote

host=Config.host
port=Config.port
user=Config.user
password=Config.password
database=Config.database

encoded_password = quote(password)
db_url = f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{database}"
engine = create_engine(db_url)

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
    "Los Angeles Clippers",
    "Los Angeles Lakers",
    "Phoenix Suns",
    "Sacramento Kings",
    "Dallas Mavericks",
    "Houston Rockets",
    "Memphis Grizzlies",
    "New Orleans Pelicans",
    "San Antonio Spurs"
]

def get_ou_value(t1,t2,date_time,df_bet_rate):
    try:
        ac=df_bet_rate[(df_bet_rate['date']==date_time)&(((df_bet_rate['team_1_name']==t1) & (df_bet_rate['team_2_name']==t2)) | ((df_bet_rate['team_1_name']==t2) & (df_bet_rate['team_2_name']==t1)))]['ov_value'].values[0]
    except Exception as e:
        # print(e)
        ac=0
    return ac

def get_spread_value(t1,t2,date_time,df_bet_rate):
    try:
        ac=df_bet_rate[(df_bet_rate['date']==date_time)&(((df_bet_rate['team_1_name']==t1) & (df_bet_rate['team_2_name']==t2)) | ((df_bet_rate['team_1_name']==t2) & (df_bet_rate['team_2_name']==t1)))]['spread'].values[0]
        if ac is None:
            ac=0
    except Exception as e:
        # print(e)
        ac=0
    return ac

def run_processing_data_merge_bet_rate(folder_save):
    df=pd.read_csv(f'{folder_save}/data_train.csv')
    sql_bet_rate= f"SELECT * FROM {database}.betrate"
    df_bet_rate=pd.read_sql_query(sql_bet_rate,engine,index_col='id')
    list_date_bet_rate = df_bet_rate['date'].unique()[:1]
    df_bet_rate.loc[df_bet_rate['team_1_name'] == 'Los Angeles Clippers', 'team_1_name'] = 'LA Clippers'
    df_bet_rate.loc[df_bet_rate['team_2_name'] == 'Los Angeles Clippers', 'team_2_name'] = 'LA Clippers'
    df['VALUE_BET_RATE'] = df.apply(lambda x: get_ou_value(x['team_1'],x['team_2'],x['day'],df_bet_rate),axis=1)
    df['VALUE_SPREAD'] = df.apply(lambda x: get_spread_value(x['team_1'],x['team_2'],x['day'],df_bet_rate),axis=1)
    df['VALUE_SPREAD'].fillna(0,inplace=True)
    df.to_csv(f'{folder_save}/result.csv',index=False)