def run_push_prediction_db():
    from NBA.config import Config
    import requests
    from sqlalchemy import create_engine,text
    from urllib.parse import quote
    from NBA.utils import date2idx, id2team
    from NBA.d_prediction import get_prediction
    
    import datetime
    import pandas as pd
    import datetime
    import os
        
    host=Config.host_app
    port=Config.port
    user=Config.user
    password=Config.password
    database=Config.database_app
    encoded_password = quote(password)
    now = datetime.datetime.now()
    add_time = datetime.timedelta(days=2)
    now = (now - add_time).strftime("%Y%m%d")

    folder_save = f'./NBA_data/data/{now}'
    # data_json={
    #     "game_id": [],
    #     "kick_off": [],
    #     "team_1": [],
    #     "team_2": [],
    #     "team_1_score_predict": [],
    #     "team_2_score_predict": [],
    # }
    # df = pd.DataFrame(data_json)
    now = datetime. datetime.now()
    add_time = datetime.timedelta(days=0)
    date = (now + add_time).strftime("%d-%m-%Y")
    idx = date2idx(date)
    headers = {
        'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MTEwNjY0LCJjbGFpbXMiOlsicCJdLCJpYXQiOjE3MDA0NTQ0MjQsImV4cCI6MTcxNjI2NTYyNH0.3r1eQ5-EvUlyyMX_RUJ9VHmEneuuxiBbtlwsy4hhChk',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
    }
    
    folder_save_log= f'./NBA_data/log/{date}_log.log'
    
    with open(folder_save_log, 'a+') as f:
        f.write(f"=================================={now}==================================\n")
        f.close()

    for i in range(idx, 175):
        data_json={
            "game_id": [],
            "kick_off": [],
            "team_1": [],
            "team_2": [],
            "team_1_score_predict": [],
            "team_2_score_predict": [],
        }
        df = pd.DataFrame(data_json)
        with open(folder_save_log, 'a+') as f:
            f.write(f"Processing date: {date}: {i}...\n")
        print(f"Processing date: {date}: {i}...")
        response = requests.get(f'https://api.nflpickwatch.com/v1/general/games/2023/{i}/nba', headers=headers)
        # print(response)
        data = response.json()
        print(f"Processing update database:...")
        try:
            for game in data:
                print(game['id'])
                game_id = game['id']
                team1 = id2team(game['home_team_id'])
                team2 = id2team(game['road_team_id'])
                
                if team1 == 'Los Angeles Clippers':
                    team1 = 'LA Clippers'
                if team2 == 'Los Angeles Clippers':
                    team2 = 'LA Clippers'
                    
                # team1 = get_team_name_same(team1)
                # team2 = get_team_name_same(team2)
                # print(team1, team2)
                df_predict = get_prediction(team1, team2,folder_save)
                df_predict.rename(columns={'Team_1_Score_predict':'team_1_score_predict', 'Team_2_Score_predict':'team_2_score_predict','Team_1':'team_1','Team_2':'team_2'}, inplace=True)
                df_predict['game_id'] = int(game_id)
                df_predict['kick_off'] = game['kickoff'].split('T')[0]
                del df_predict['Total_Predict_Score']
                df=pd.concat([df,df_predict])
                
            
            with create_engine("mysql+pymysql://"+user+":"+encoded_password+"@"+host+":"+str(port)+"/"+database,future=True).connect() as conn:
                try:
                    list_game_id_database = pd.read_sql('SELECT game_id FROM history_predict', con=conn)['game_id'].tolist()
                    for index, row in df.iterrows():
                        if row['game_id'] in list_game_id_database:
                            print(f"Update game_id: {row['game_id']}")
                            update_query = text(f"UPDATE db_nba_pre.history_predict SET team_1_score_predict = {row['team_1_score_predict']}, team_2_score_predict = {row['team_2_score_predict']} WHERE game_id = {int(row['game_id'])}")        
                            conn.execute(update_query)
                            with open(folder_save_log, 'a') as f:
                                f.write(f"Update database successfully! Date: {date} Game_id: {str(int(row['game_id']))}\n")
                                f.close()
                        else:
                            print(f"Insert game_id: {row['game_id']}")
                            insert_query = text(f"INSERT INTO db_nba_pre.history_predict (game_id, kick_off, team_1, team_2, team_1_score_predict, team_2_score_predict) VALUES ({int(row['game_id'])}, '{row['kick_off']}', '{row['team_1']}', '{row['team_2']}', {row['team_1_score_predict']}, {row['team_2_score_predict']})")
                            conn.execute(insert_query)
                            with open(folder_save_log, 'a') as f:
                                f.write(f"Insert database successfully! Date: {date} Game_id: {str(int(row['game_id']))}\n")
                                f.close()
                        conn.commit()
                except Exception as e:
                    print(e)
                    print("Update database failed!")
                    with open(folder_save_log, 'a') as f:
                        f.write(f"Update database failed! Date: {date} Game_id: {game_id} Error: {e}\n")
                        f.close()
        except Exception as e:
            print(e)
            print("Update database failed!")
            with open(folder_save_log, 'a') as f:
                f.write(f"Update database failed! Date: {date} - Error: {e}\n")
                f.close()
        add_time_to = datetime.timedelta(days=1)
        date = (datetime.datetime.strptime(date, "%d-%m-%Y") + add_time_to).strftime("%d-%m-%Y")