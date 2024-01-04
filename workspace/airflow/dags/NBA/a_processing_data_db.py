from sqlalchemy import create_engine
import pandas as pd
from urllib.parse import quote
from NBA.config import Config
import re


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

column_basic=['game_id','team_1','team_2','t1_score','t2_score']

column_info = ['day','team_name','player_name']

column_advanced=['offrtg','defrtg','netrtg','ast_percent','ast_ratio','oreb_percent','dreb_percent','reb_percent','efg_percent','ts_percent']

column_defense=['def_min','partial_poss','pts','dreb', 'ast','dfgm', 'dfga','d3pa', 'd3p_percent']

column_traditional=['fgm', 'fga', '3pm', '3pa', '3p_percent', 'ftm', 'fta', 'ft_percent', 'oreb', 'dreb', 'reb', 'pts']

column_fourfactors=['fta_rate', 'tm_to_percent', 'opp_efg_percent', 'opp_fta_rate', 'opp_to_percent','opp_oreb_percent']

column_hustle= ['screen_ast', 'screen_ast_pts', 'off_loose_balls_recovered', 'def_loose_balls_recovered', 'loose_balls_recovered', 'contested_2pt_shots', 'contested_3pt_shots', 'contested_shots', 'off_box_outs', 'def_box_outs', 'box_outs']

column_misc=['min','opp_pitp']

column_scoring=['percent_fga_2pt','percent_fga_3pt', 'percent_pts_2pt','percent_pts_3pt','percent_pts_pitp','2fgm_percent_ast','2fgm_percent_uast','3fgm_percent_ast','fgm_percent_ast', 'fgm_percent_uast']

column_tracking=['spd', 'dist', 'orbc','drbc', 'rbc', 'tchs', 'pass', 'ast', 'cfgm', 'cfga', 'cfg_percent', 'ufgm', 'ufga', 'ufg_percent', 'fg_percent', 'dfgm', 'dfga', 'dfg_percent']

column_usage=['usg_percent', 'percent_fgm', 'percent_fga', 'percent_3pm', 'percent_3pa', 'percent_ftm', 'percent_fta', 'percent_dreb', 'percent_reb', 'percent_pfd', 'percent_pts']

fn2abb={
    'advanced':'adv',
    'defense':'def',
    'fourfactors':'ff',
    'hustle':'hus',
    'misc':'mis',
    'scoring':'sco',
    'tracking':'trac',
    'usage':'usa',
    'traditional':'trad',
    'history':'gh',
}
fn2table={
    'advanced':'boxscores_advanced',
    'defense':'boxscores_defense',
    'fourfactors':'boxscores_fourfactors',
    'hustle':'boxscores_hustle',
    'misc':'boxscores_misc',
    'scoring':'boxscores_scoring',
    'tracking':'boxscores_tracking',
    'usage':'boxscores_usage',
    'traditional':'boxscores_traditional',
    'history':'game_history',
}

column_group = column_basic + column_info

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

def get_column(df_name):
    if df_name == 'advanced':
        return column_info+column_advanced
    elif df_name == 'misc':
        return column_info+column_misc
    elif df_name == 'defense':
        return column_info+column_defense
    elif df_name == 'scoring':
        return column_info+column_scoring
    elif df_name == 'fourfactors':
        return column_info+column_fourfactors
    elif df_name == 'tracking':
        return column_info+column_tracking
    elif df_name == 'hustle':
        return column_info+column_hustle
    elif df_name == 'traditional':
        return column_info+column_traditional
    elif df_name == 'usage':
        return column_info+column_usage
    
def get_select_query(column_query,table_name_qr,table_name_basic='gh',column_basic=column_basic):
    text_column_query = ""
    for i in range(len(column_basic)):
        text_column_query += f'{table_name_basic}.{column_basic[i]},'
    for i in range(len(column_query)):
        if i == len(column_query)-1:
            text_column_query += f'{table_name_qr}.{column_query[i]}'
        else:
            text_column_query += f'{table_name_qr}.{column_query[i]},'
    return text_column_query

def query_train(table_name='advanced',engine=engine,fn2abb=fn2abb,fn2table=fn2table,databases=database,table_join='game_history'):
    get_column_query=get_column(table_name)
    columns_query = get_select_query(column_query=get_column_query,table_name_qr=fn2abb[table_name])
    
    sql_his_ad= f"SELECT {columns_query} FROM {databases}.{table_join} as gh join {databases}.{fn2table[table_name]} as {fn2abb[table_name]} on gh.game_id = {fn2abb[table_name]}.game_id where year in (2021,2022,2023)"
    
    # sql_his_ad= f"SELECT {columns_query} FROM db_nba_staging.{table_join} as gh join db_nba_staging.{fn2table[table_name]} as {fn2abb[table_name]} on gh.game_id = {fn2abb[table_name]}.game_id where STR_TO_DATE({fn2abb[table_name]}.day, '%d-%m-%Y') BETWEEN '2021-10-20' AND '2023-11-24'"
    
    # sql_his_ad= "SELECT "+columns_query+" FROM db_nba_staging.game_history as gh join db_nba_staging."+fn2table[table_name]+" as "+fn2abb[table_name]+" on gh.game_id = "+fn2abb[table_name]+".game_id where STR_TO_DATE("+fn2abb[table_name]+".day, '%d-%m-%Y') BETWEEN '2021-10-20' AND '2023-11-24';"
    
    df_his_ad= pd.read_sql_query(sql_his_ad, engine)
    df_his_ad = df_convert_min(df_his_ad)
    # fillnan 0
    list_game_id = df_his_ad['game_id'].unique().tolist()[0]
    df_his_ad_test = df_his_ad[df_his_ad['game_id'] == list_game_id]
    if df_his_ad_test['player_name'].count() > 30:
        columns = df_his_ad.columns
        if 'pts' in columns:
            # print(columns)
            mean_cols = columns[8:-1]
            sum_cols = columns[-1]
            agg_dict = {**{col: 'mean' for col in mean_cols}, sum_cols: 'sum'}
            df_his_ad=df_his_ad.groupby(["game_id","team_1","team_2","t1_score","t2_score","day","team_name","player_name"]).agg(agg_dict).reset_index()
        else:
            df_his_ad=df_his_ad.groupby(["game_id","team_1","team_2","t1_score","t2_score","day","team_name","player_name"]).mean().reset_index()
    df_his_ad.replace('Los Angeles Clippers','LA Clippers',inplace=True)
    return df_his_ad.round(2)

def convert_day(x):
    list_x = x.split('-')
    if len(list_x[0])==4:
        return x
    else:
        x=list_x[2]+'-'+list_x[1]+'-'+list_x[0]
        return x

def run_processing_data_db(folder_save):
    for i,table_name in enumerate(list(fn2abb.keys())[:-1]):
        print(i,table_name)
        if i == 0:
            df_merge=query_train(table_name='advanced')
        else:
            df=query_train(table_name=table_name)
            # đổi Los Angeles Lakers thành LA Lakers
            if table_name == 'defense':
                df.rename(columns={'pts':'def_pts','dreb':'def_dreb','ast':'def_ast','dfgm':'def_dfgm','dfga':'def_fga','d3pa':'def_3pa','d3p_percent':'d3p_percent'}, inplace=True)
            df_merge=df_merge.merge(df,on=['game_id','team_1','team_2','t1_score','t2_score','day','team_name','player_name'],how='left')
    df_merge.fillna(0,inplace=True)
    mean_cols = df_merge.columns[8:-1]
    sum_cols = df_merge.columns[-1]
    agg_dict = {**{col: 'mean' for col in mean_cols}, sum_cols: 'sum'}
    df_merge=df_merge.groupby(["game_id","team_1","team_2","t1_score","t2_score","day","team_name","player_name"]).mean().reset_index()
    
    df_check_game_id = df_merge.copy()
    df_check_game_id['total_score'] = df_check_game_id['t1_score'] + df_check_game_id['t2_score']
    df_check_game_id = df_check_game_id[['game_id','total_score','pts']]
    agg_dict = {'total_score': 'mean', 'pts': 'sum'}
    df_check_game_id=df_check_game_id.groupby(["game_id"]).agg(agg_dict).reset_index()
    df_check_game_id['tf']=df_check_game_id['total_score']-df_check_game_id['pts']
    game_id_none = df_check_game_id[df_check_game_id['tf'] != 0].game_id.to_list()
    df_merge=df_merge[~df_merge['game_id'].isin(game_id_none)].reset_index(drop=True).round(2)
    
    df_merge['day']=df_merge['day'].apply(convert_day)
    df_merge.to_csv(f'{folder_save}/data_train.csv',index=False)