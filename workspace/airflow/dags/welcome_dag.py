from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from NBA.bet_rate import *
import pandas as pd
from NBA.send_mail import run_send_mail
from NBA.run_pipeline import run_pipeline
from NBA.e_push_prediction_db import run_push_prediction_db
from NBA.push import push_nba
#_______________________________________________________DAG-NBA_______________________________________________________
dag = DAG(
    'NBA',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)



bet_rate = PythonOperator(
    task_id='bet_rate',
    python_callable=bet_rate,
    dag=dag
)

boxscores_advanced = PythonOperator(
    task_id='boxscores_advanced',
    python_callable=boxscores_advanced,
    dag=dag
)

boxscores_defense = PythonOperator(
    task_id='boxscores_defense',
    python_callable=boxscores_defense,
    dag=dag
)

boxscores_fourfactors = PythonOperator(
    task_id='boxscores_fourfactors',
    python_callable=boxscores_fourfactors,
    dag=dag
)

boxscores_hustle = PythonOperator(
    task_id='boxscores_hustle',
    python_callable=boxscores_hustle,
    dag=dag 
)
boxscores_misc = PythonOperator(
    task_id='boxscores_misc',
    python_callable=boxscores_misc,
    dag=dag
)
boxscores_scoring = PythonOperator(
    task_id='boxscores_scoring',
    python_callable=boxscores_scoring,
    dag=dag
)
                                    
boxscores_tracking = PythonOperator(
    task_id='boxscores_tracking',
    python_callable=boxscores_tracking,
    dag=dag
)

boxscores_traditional = PythonOperator(
    task_id='boxscores_traditional',
    python_callable=boxscores_traditional,
    dag=dag
)

boxscores_usage = PythonOperator(
    task_id='boxscores_usage',
    python_callable=boxscores_usage,
    dag=dag
)

boxscores = PythonOperator(
    task_id='boxscores',
    python_callable=boxscores,
    dag=dag
)

game_history = PythonOperator(    
    task_id='game_history',
    python_callable=game_history,
    dag=dag
)

push_nba = PythonOperator(
    task_id='push_nba',
    python_callable=push_nba,
    dag=dag
)

run_pipeline_model = PythonOperator(
    task_id='run_pipeline',
    python_callable=run_pipeline,
    dag=dag
)

run_send_mail_user = PythonOperator(
    task_id="run_send_mail_user",
    python_callable = run_send_mail,
    dag = dag
)

run_push_prediction_db_aws = PythonOperator(
    task_id="run_push_prediction_db",
    python_callable = run_push_prediction_db,
    dag = dag
)
#__________________________________________________Set the dependencies between the tasks_______________________________________________________

# [bet_rate, boxscores_advanced, boxscores_defense, boxscores_fourfactors, boxscores_hustle, boxscores_misc, boxscores_scoring, boxscores_tracking, boxscores_traditional, boxscores_usage, boxscores, game_history] >> push_nba >> run_pipeline_model>> run_send_mail_user>>run_push_prediction_db_aws

# run_push_prediction_db_aws
# run_send_mail_user
[bet_rate, boxscores_advanced, boxscores_defense, boxscores_fourfactors, boxscores_hustle, boxscores_misc, boxscores_scoring,
boxscores_tracking, boxscores_traditional, boxscores_usage, boxscores, game_history] >> push_nba >> run_pipeline_model>> run_send_mail_user>>run_push_prediction_db_aws
# run_send_mail_user #>> run_push_prediction_db_aws
#16:07:03 đến 18:40:41

#_______________________________________________________DAG-MLB_______________________________________________________


dag_mlb = DAG(
    'MLB',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

from MLB.player_stat import player_stat
from MLB.rank_team import rank_team
from MLB.team_stats import team_stats   
from MLB.injury_info import injury_info
from MLB.bet_rate import bet_rate
from MLB.team import team

player_stat = PythonOperator(
    task_id='player_stat',
    python_callable=player_stat,
    dag=dag_mlb
)

rank_team = PythonOperator(
    task_id='rank_team',
    python_callable=rank_team,
    dag=dag_mlb
)

team_stats = PythonOperator(
    task_id='team_stats',
    python_callable=team_stats,
    dag=dag_mlb
)

injury_info = PythonOperator(
    task_id='injury_info',
    python_callable=injury_info,
    dag=dag_mlb
)

bet_rate = PythonOperator(
    task_id='bet_rate',
    python_callable=bet_rate,
    dag=dag_mlb
)

team = PythonOperator(
    task_id='team',
    python_callable=team,
    dag=dag_mlb
)
#__________________________________________________NO END_______________________________________________________