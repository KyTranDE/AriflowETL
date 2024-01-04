def push_nba():
    from NBA.mysql_tool import MySql
    import pandas as pd

    host = "*********"
    port = "*********"
    user = "*********"
    password = "*********"
    database = "*********"

    mydb = MySql(host, user, port, password, database)
    mydb.connect()

#_______________________________________________________def dataframe_from_query_______________________________________________________
    def dataframe_from_query(query):
        result = mydb.query(query, False)
        list_day, list_game_id, list_team_name, list_player_name, list_team_id = zip(*result)
        df = pd.DataFrame({
            'day': list_day,
            'game_id': list_game_id,
            'team_name': list_team_name,
            'player_name': list_player_name,
            'team_id': list_team_id
        })
        return df
#_______________________________________________________betrate_______________________________________________________
    df = pd.read_csv('./data/betrate.csv')  
    for i in range(len(df)):
            select = mydb.query(f"SELECT * FROM betrate WHERE date = '{df['date'][i]}'and team_1_id = {df['team_1_id'][i]} and team_1_name = '{df['team_1_name'][i]}' and team_2_id = {df['team_2_id'][i]} and team_2_name = '{df['team_2_name'][i]}' ", False)
            if len(select) > 0:
                # nếu lớn hơn 0 thì xóa dòng dataframr đó
                df = df.drop(i)
    print(mydb.query(f"SELECT * FROM betrate ", False))

    cont = mydb.query(f'SELECT id FROM betrate ORDER BY id DESC LIMIT 1', False)
    count_id =  int(cont[0][0])+1

    df['id'] = [int(i) for i in range(count_id  , count_id  +len(df))]
    df.to_csv('./data/betrate.csv')
    mydb.push_data('./data/betrate.csv')

#_______________________________________________________boxscores_traditional_______________________________________________________
    df = pd.read_csv('./data/boxscores_traditional.csv')

    compare = dataframe_from_query("SELECT day, game_id, team_name, player_name,  team_id FROM boxscores_traditional")

    merged_df = df.merge(compare, on=['day', 'game_id', 'team_name', 'player_name', 'team_id'], how='inner')
    if not merged_df.empty:
        print("Các dòng sẽ bị xóa:")
        print(merged_df)
        rows_to_drop = df[df.isin(merged_df.to_dict(orient='list')).all(axis=1)]
        print(rows_to_drop)
        df.drop(rows_to_drop.index, inplace=True)
        print(f"Đã xóa {len(rows_to_drop)} hàng")
    else:
        print("Không có dòng nào sẽ bị xóa.")

    cont = mydb.query('SELECT id FROM boxscores_traditional ORDER BY id DESC LIMIT 1', False)
    count_id = int(cont[0][0]) + 1

    df['id'] = [int(i) for i in range(count_id, count_id + len(df))]
    df.to_csv('./data/boxscores_traditional.csv')
    mydb.push_data('./data/boxscores_traditional.csv')
    
#_______________________________________________________game_history_______________________________________________________
    mydb.push_data('./data/game_history.csv')



#_______________________________________________________boxscores_advanced_______________________________________________________
    df = pd.read_csv('./data/boxscores_advanced.csv')

    compare = dataframe_from_query("SELECT day, game_id, team_name, player_name,  team_id FROM boxscores_advanced")

    merged_df = df.merge(compare, on=['day', 'game_id', 'team_name', 'player_name', 'team_id'], how='inner')
    if not merged_df.empty:
        print("Các dòng sẽ bị xóa:")
        print(merged_df)
        rows_to_drop = df[df.isin(merged_df.to_dict(orient='list')).all(axis=1)]
        print(rows_to_drop)
        df.drop(rows_to_drop.index, inplace=True)
        print(f"Đã xóa {len(rows_to_drop)} hàng")
    else:
        print("Không có dòng nào sẽ bị xóa.")

    cont = mydb.query('SELECT id FROM boxscores_advanced ORDER BY id DESC LIMIT 1', False)
    count_id = int(cont[0][0]) + 1

    df['id'] = [int(i) for i in range(count_id, count_id + len(df))]
    df.to_csv('./data/boxscores_advanced.csv')
    mydb.push_data('./data/boxscores_advanced.csv')

#_______________________________________________________boxscores_defense_______________________________________________________

    df = pd.read_csv('./data/boxscores_defense.csv')

    compare = dataframe_from_query("SELECT day, game_id, team_name, player_name,  team_id FROM boxscores_defense")

    merged_df = df.merge(compare, on=['day', 'game_id', 'team_name', 'player_name', 'team_id'], how='inner')
    if not merged_df.empty:
        print("Các dòng sẽ bị xóa:")
        print(merged_df)
        rows_to_drop = df[df.isin(merged_df.to_dict(orient='list')).all(axis=1)]
        print(rows_to_drop)
        df.drop(rows_to_drop.index, inplace=True)
        print(f"Đã xóa {len(rows_to_drop)} hàng")
    else:
        print("Không có dòng nào sẽ bị xóa.")

    cont = mydb.query('SELECT id FROM boxscores_defense ORDER BY id DESC LIMIT 1', False)
    count_id = int(cont[0][0]) + 1

    df['id'] = [int(i) for i in range(count_id, count_id + len(df))]
    df.to_csv('./data/boxscores_defense.csv')
    mydb.push_data('./data/boxscores_defense.csv')

#_______________________________________________________boxscores_fourfactors_______________________________________________________

    df = pd.read_csv('./data/boxscores_fourfactors.csv')

    compare = dataframe_from_query("SELECT day, game_id, team_name, player_name,  team_id FROM boxscores_fourfactors")

    merged_df = df.merge(compare, on=['day', 'game_id', 'team_name', 'player_name', 'team_id'], how='inner')
    if not merged_df.empty:
        print("Các dòng sẽ bị xóa:")
        print(merged_df)
        rows_to_drop = df[df.isin(merged_df.to_dict(orient='list')).all(axis=1)]
        print(rows_to_drop)
        df.drop(rows_to_drop.index, inplace=True)
        print(f"Đã xóa {len(rows_to_drop)} hàng")
    else:
        print("Không có dòng nào sẽ bị xóa.")

    cont = mydb.query('SELECT id FROM boxscores_fourfactors ORDER BY id DESC LIMIT 1', False)
    count_id = int(cont[0][0]) + 1

    df['id'] = [int(i) for i in range(count_id, count_id + len(df))]
    df.to_csv('./data/boxscores_fourfactors.csv')
    mydb.push_data('./data/boxscores_fourfactors.csv')

#_______________________________________________________boxscores_hustle_______________________________________________________
    df = pd.read_csv('./data/boxscores_hustle.csv')

    compare = dataframe_from_query("SELECT day, game_id, team_name, player_name,  team_id FROM boxscores_hustle")

    merged_df = df.merge(compare, on=['day', 'game_id', 'team_name', 'player_name', 'team_id'], how='inner')
    if not merged_df.empty:
        print("Các dòng sẽ bị xóa:")
        print(merged_df)
        rows_to_drop = df[df.isin(merged_df.to_dict(orient='list')).all(axis=1)]
        print(rows_to_drop)
        df.drop(rows_to_drop.index, inplace=True)
        print(f"Đã xóa {len(rows_to_drop)} hàng")
    else:
        print("Không có dòng nào sẽ bị xóa.")

    cont = mydb.query('SELECT id FROM boxscores_hustle ORDER BY id DESC LIMIT 1', False)
    count_id = int(cont[0][0]) + 1

    df['id'] = [int(i) for i in range(count_id, count_id + len(df))]
    df.to_csv('./data/boxscores_hustle.csv')
    mydb.push_data('./data/boxscores_hustle.csv')


#_______________________________________________________boxscores_misc_______________________________________________________

    df = pd.read_csv('./data/boxscores_misc.csv')

    compare = dataframe_from_query("SELECT day, game_id, team_name, player_name,  team_id FROM boxscores_misc")

    merged_df = df.merge(compare, on=['day', 'game_id', 'team_name', 'player_name', 'team_id'], how='inner')
    if not merged_df.empty:
        print("Các dòng sẽ bị xóa:")
        print(merged_df)
        rows_to_drop = df[df.isin(merged_df.to_dict(orient='list')).all(axis=1)]
        print(rows_to_drop)
        df.drop(rows_to_drop.index, inplace=True)
        print(f"Đã xóa {len(rows_to_drop)} hàng")
    else:
        print("Không có dòng nào sẽ bị xóa.")

    cont = mydb.query('SELECT id FROM boxscores_misc ORDER BY id DESC LIMIT 1', False)
    count_id = int(cont[0][0]) + 1

    df['id'] = [int(i) for i in range(count_id, count_id + len(df))]
    df.to_csv('./data/boxscores_misc.csv')
    mydb.push_data('./data/boxscores_misc.csv')

#_______________________________________________________boxscores_scoring_______________________________________________________
    df = pd.read_csv('./data/boxscores_scoring.csv')

    compare = dataframe_from_query("SELECT day, game_id, team_name, player_name,  team_id FROM boxscores_scoring")

    merged_df = df.merge(compare, on=['day', 'game_id', 'team_name', 'player_name', 'team_id'], how='inner')
    if not merged_df.empty:
        print("Các dòng sẽ bị xóa:")
        print(merged_df)
        rows_to_drop = df[df.isin(merged_df.to_dict(orient='list')).all(axis=1)]
        print(rows_to_drop)
        df.drop(rows_to_drop.index, inplace=True)
        print(f"Đã xóa {len(rows_to_drop)} hàng")
    else:
        print("Không có dòng nào sẽ bị xóa.")

    cont = mydb.query('SELECT id FROM boxscores_scoring ORDER BY id DESC LIMIT 1', False)
    count_id = int(cont[0][0]) + 1

    df['id'] = [int(i) for i in range(count_id, count_id + len(df))]
    df.to_csv('./data/boxscores_scoring.csv')
    mydb.push_data('./data/boxscores_scoring.csv')


#_______________________________________________________boxscores_tracking_______________________________________________________

    df = pd.read_csv('./data/boxscores_tracking.csv')

    compare = dataframe_from_query("SELECT day, game_id, team_name, player_name,  team_id FROM boxscores_tracking")

    merged_df = df.merge(compare, on=['day', 'game_id', 'team_name', 'player_name', 'team_id'], how='inner')
    if not merged_df.empty:
        print("Các dòng sẽ bị xóa:")
        print(merged_df)
        rows_to_drop = df[df.isin(merged_df.to_dict(orient='list')).all(axis=1)]
        print(rows_to_drop)
        df.drop(rows_to_drop.index, inplace=True)
        print(f"Đã xóa {len(rows_to_drop)} hàng")
    else:
        print("Không có dòng nào sẽ bị xóa.")

    cont = mydb.query('SELECT id FROM boxscores_tracking ORDER BY id DESC LIMIT 1', False)
    count_id = int(cont[0][0]) + 1

    df['id'] = [int(i) for i in range(count_id, count_id + len(df))]
    df.to_csv('./data/boxscores_tracking.csv')
    mydb.push_data('./data/boxscores_tracking.csv')


#_______________________________________________________boxscores_usage_______________________________________________________

    df = pd.read_csv('./data/boxscores_usage.csv')

    compare = dataframe_from_query("SELECT day, game_id, team_name, player_name,  team_id FROM boxscores_usage")

    merged_df = df.merge(compare, on=['day', 'game_id', 'team_name', 'player_name', 'team_id'], how='inner')
    if not merged_df.empty:
        print("Các dòng sẽ bị xóa:")
        print(merged_df)
        rows_to_drop = df[df.isin(merged_df.to_dict(orient='list')).all(axis=1)]
        print(rows_to_drop)
        df.drop(rows_to_drop.index, inplace=True)
        print(f"Đã xóa {len(rows_to_drop)} hàng")
    else:
        print("Không có dòng nào sẽ bị xóa.")

    cont = mydb.query('SELECT id FROM boxscores_usage ORDER BY id DESC LIMIT 1', False)
    count_id = int(cont[0][0]) + 1

    df['id'] = [int(i) for i in range(count_id, count_id + len(df))]
    df.to_csv('./data/boxscores_usage.csv')
    mydb.push_data('./data/boxscores_usage.csv')

    
    
    
    
# _______________________________________________________kill all csv_______________________________________________________
    import os
    try:
        os.remove('./data/betrate.csv')
        os.remove('./data/boxscores_advanced.csv')
        os.remove('./data/boxscores_defense.csv')
        os.remove('./data/boxscores_fourfactors.csv')
        os.remove('./data/boxscores_hustle.csv')
        os.remove('./data/boxscores_misc.csv')
        os.remove('./data/boxscores_scoring.csv')
        os.remove('./data/boxscores_tracking.csv')
        os.remove('./data/boxscores_traditional.csv')
        os.remove('./data/boxscores_usage.csv')
        os.remove('./data/boxscores.csv')
        os.remove('./data/game_history.csv')
    except OSError as e:
        print(e)

