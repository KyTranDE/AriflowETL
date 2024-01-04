import sys
sys.path.append('../')
import pandas as pd
import numpy as np
from config.params import *
from utils.logging import *
from utils.utils import write_log
import mysql.connector
from tabulate import tabulate
from tqdm import tqdm

class MySql():
    def __init__(self, host=host, user=user, port=port, password=password, database=database):
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.database = database
        self.auth_plugin = 'mysql_native_password'
        self.db = None
        self.cursor = None
        self.columns = None
        self.schema = []
        self.history_query = []

    def connect(self):
        '''
        use to connect to MySQL
        '''
        try:
            self.db = mysql.connector.connect(
                host=self.host,
                user=self.user,
                port=self.port,
                password=self.password,
                database=self.database,
                auth_plugin=self.auth_plugin
            )
            self.cursor = self.db.cursor()
            print(OK('Connect to MySQL successfully!'))
        except Exception as e:
            print(e)
    
    def disconnect(self):
        '''
        use to disconnect to MySQL
        '''
        try:
            self.cursor.close()
            self.db.close()
            print(OK('Disconnect to MySQL successfully! Bye.'))
        except Exception as e:
            print(e)

    def query(self, text, show=True, fmt = 'psql'):
        '''
        use to query to MySQL
        '''
        self.history_query.append(text)
        self.cursor.execute(text)
        tables = self.cursor.fetchall()
        if show:
            print(tabulate(tables, headers=self.cursor.column_names, tablefmt=fmt))
        else:
            return tables
    
    def create_schema(self, sql_path, show=True):
        '''
        use to create tables on the database and link them together
        '''
        with open(sql_path, 'r') as f:
            sql_statements = f.read()
            sql_statements = sql_statements.split('\n\n')
            self.schema = sql_statements

        check = False
        try:
            for statement in self.schema:
                self.cursor.execute(statement)
                if statement.find('CREATE TABLE') != -1:
                    print(f'Creating table {TEXT(statement.split("`")[1])}...')
                if statement.find('ALTER TABLE') != -1:
                    alter = statement.split("`")
                    print(f'Linking table {TEXT(alter[1])} -> {TEXT(alter[5])}')
            check = True
            self.db.commit()
            if show:
                self.query('SHOW TABLES;')
        except Exception as e:
            print(e)
        print(OK('Done!') if check else FAIL('Fail!'))
    
    def get_data(self, path_csv):
        '''
        use to get data from csv file
        '''
        df = pd.read_csv(path_csv)
        df = df[self.columns]
        data = np.where(pd.isna(df), None, df).tolist()
        data = [tuple(i) for i in data]
        return data
        
    def push_data(self, src: str = None, table: str = None, data: list = None):
        '''
        use to push data to database
        '''
        if src:
            table = src.split('/')[-1].split('.')[0]
        cols = self.query(f'SHOW COLUMNS FROM {table};', show=False)
        cols = [i[0] for i in cols]
        self.columns = cols
        if src:
            data = self.get_data(src)
        sql_insert = f"insert into {table} ({','.join(cols)}) values ({','.join(['%s']*len(cols))})"
        loop = tqdm(range(len(data)), desc=f"Inserting {TEXT_BOLD(table)}...", colour='cyan', ncols=100)
        misses = 0
        for l in loop:
            try:
                self.cursor.execute(sql_insert, data[l])
                loop.set_postfix(status=f'{OK("success")}')
                # write_log(f'../log/push/{table}.log', f'success')
            except Exception as e:
                write_log(f'../log/push/{table}.log', f'fail: {e}')
                loop.set_postfix(status=f'{FAIL("Fail")} at line {l}: {e}')
                misses += 1
                continue
        print(OK(f"Success push {len(data)-misses} rows to {table}!"))
        # print(OK(f"Done push data to {table}!") if misses!=len(data) else FAIL(f"Not push data to{table}!"))
        self.db.commit()