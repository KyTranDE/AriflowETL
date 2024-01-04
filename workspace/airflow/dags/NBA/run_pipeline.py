def run_pipeline():
    from NBA.a_processing_data_db import run_processing_data_db
    from NBA.b_three_processing_data_merge_bet_rate import run_processing_data_merge_bet_rate
    from NBA.c_four_train_model import run_train_model
    import joblib
    import json

    import datetime
    import os
    now = datetime.datetime.now()
    add_time = datetime.timedelta(days=2)
    now = (now - add_time).strftime("%Y%m%d")

    folder_save = f'NBA_data/data/{now}'
    if not os.path.exists(folder_save):
        os.makedirs(folder_save)

    print("run_processing_data_db")
    run_processing_data_db(folder_save)
    print("run_processing_data_merge_bet_rate")
    run_processing_data_merge_bet_rate(folder_save)
    print("run_train_model")
    results=run_train_model(folder_save)
    print(results)
    # lưu result vào file txt
    result = json.dumps(results, indent=4)

    with open(f'{folder_save}/{now}.txt', 'w') as f:
        f.write(result)
