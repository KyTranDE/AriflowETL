from NBA.config import Config
import numpy as np
import pandas as pd

from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_absolute_error,r2_score  

import joblib

def accuracy_bet_rate(test_target,predicted_labels,test_ov):
    win=0
    lose=0
    for i in range(len(test_target)):
        if predicted_labels[i] > test_ov[i]:
            if test_target[i] > test_ov[i]:
                win+=1
            else:
                lose+=1
        else:
            if test_target[i] < test_ov[i]:
                win+=1
            else:
                lose+=1
    return win/(win+lose)*100

def who_win(test_target,predicted_labels,test_ov):
    if predicted_labels > test_ov:
        if test_target > test_ov:
            return 1
        else:
            return 0
    else:
        if test_target < test_ov:
            return 1
        else:
            return 0

def accuracy_spread(spread,T1_Predicted_Score,T2_Predicted_Score):
    if spread + T1_Predicted_Score > T2_Predicted_Score:
        return 1
    else:
        return 0

def run_train_model(folder_save):
    import joblib

    df=pd.read_csv(f'{folder_save}/result.csv')
    del df['t1_score']
    del df['t2_score']
    
    la_team = LabelEncoder()
    la_player = LabelEncoder()
    
    la_team.fit(df['team_name'])
    la_player.fit(df['player_name'])
    
    
    joblib.dump(la_team, f'{folder_save}/la_team_new.pkl')
    joblib.dump(la_player, f'{folder_save}/la_player_new.pkl')
    la_team = joblib.load(f'{folder_save}/la_team_new.pkl')
    la_player = joblib.load(f'{folder_save}/la_player_new.pkl')
    
    for i in df.select_dtypes(include=['object']).columns:
        try:
            if i.startswith('team'):
                df[i]=la_team.transform(df[i])
            else:
                df[i]=la_player.transform(df[i])
        except:
            continue
        # print(i)
    df_train = df[(df['VALUE_BET_RATE']==0.0) | (df['VALUE_SPREAD']==0.0)].reset_index(drop=True)
    test_game_id = df_train['game_id'].unique()
    df_test = df[~df['game_id'].isin(test_game_id)].reset_index(drop=True)
    
    test_ou=df_test['VALUE_BET_RATE']
    test_spread = df_test['VALUE_SPREAD']
    y_train = df_train['pts']
    y_test = df_test['pts']

    x_train = df_train.drop(['VALUE_BET_RATE','day','game_id','pts','VALUE_SPREAD'],axis=1)

    x_test = df_test.drop(['VALUE_BET_RATE','day','game_id','pts','VALUE_SPREAD'],axis=1)
    X_test_copy=df_test.copy()
    
    scaler = StandardScaler()
    scaler.fit(x_train)
    joblib.dump(scaler, f'{folder_save}/scaler.pkl')
    scaler = joblib.load(f'{folder_save}/scaler.pkl')
    
    x_train = scaler.transform(x_train)
    x_test = scaler.transform(x_test)
    
    model = SGDRegressor()
    model.fit(x_train, y_train)
    predict = model.predict(x_test)
    
    import joblib
    joblib.dump(model,f'{folder_save}/model.pkl')
    
    mae = mean_absolute_error(y_test, predict)
    r2 = r2_score(y_test, predict)

    for i in ['team_1', 'team_2', 'team_name', 'player_name']:
        if i.startswith('team'):
            X_test_copy[i]=la_team.inverse_transform(X_test_copy[i])
        else:
            X_test_copy[i]=la_player.inverse_transform(X_test_copy[i])
    
    X_test_copy=X_test_copy[['game_id','team_1', 'team_2','team_name']]
    X_test_copy['perdiction']=predict
    X_test_copy['score']=y_test
    X_test_copy['bet_rate']=test_ou
    X_test_copy['spread']=test_spread
    
    merged_data=X_test_copy.groupby(['game_id', 'team_1', 'team_2','team_name']).agg({
        'perdiction': 'sum',
        'score': 'sum',
        'bet_rate': 'mean',
        'spread': 'mean'
    }).reset_index()
    
    df_2 =merged_data.copy()
    # Tạo các cột mới theo yêu cầu
    df_2['t1_Score'] = df_2.apply(lambda row: row['score'] if row['team_name'] == row['team_1'] else None, axis=1)
    df_2['t2_Score'] = df_2.apply(lambda row: row['score'] if row['team_name'] == row['team_2'] else None, axis=1)
    df_2['t1_predicted_Score'] = df_2.apply(lambda row: row['perdiction'] if row['team_name'] == row['team_1'] else None, axis=1)
    df_2['t2_predicted_Score'] = df_2.apply(lambda row: row['perdiction'] if row['team_name'] == row['team_2'] else None, axis=1)

    # Loại bỏ các cột không cần thiết
    df_2 = df_2.drop(['score', 'perdiction'], axis=1)
    df_2.fillna(0,inplace=True)
    
    del df_2['team_name']
    
    df_2=df_2.groupby(['game_id','team_1','team_2']).agg({
        'bet_rate': 'mean',
        'spread': 'mean',
        't1_Score': 'sum',
        't2_Score': 'sum',
        't1_predicted_Score': 'sum',
        't2_predicted_Score': 'sum'
    }).reset_index()
    
    df_2['Win']=df_2.apply(lambda x: who_win(x['t1_Score']+ x['t2_Score'],x['t1_predicted_Score'] + x['t2_predicted_Score'],x['bet_rate']),axis=1)
    
    df_2['Win_True_spread']=df_2.apply(lambda row: accuracy_spread(row['spread'],row['t1_Score'],row['t2_Score']),axis=1)
    df_2['Win_predict_spread']=df_2.apply(lambda row: accuracy_spread(row['spread'],row['t1_predicted_Score'],row['t2_predicted_Score']),axis=1)
    df_2.to_csv(f'{folder_save}/prediction.csv',index=False)
    return {
        "mae":mae,
        "r2":r2,
        "bet_rate_accuracy": df_2['Win'].sum()/len(df_2)*100,
        "spread_accuracy": (df_2['Win_True_spread'] == df_2['Win_predict_spread']).sum()/len(df_2)*100,
    }