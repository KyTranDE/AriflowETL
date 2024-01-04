import pandas as pd
import numpy as np
import mysql.connector
from tqdm import tqdm
from tabulate import tabulate

B = '\033[94m'
Y = '\033[93m'
G = '\033[92m'
R = '\033[91m'
BOLD = '\033[1m'
E = '\033[0m'

def FAIL(msg='ERROR'):
    return R + str(msg) + E

def OK(msg='SUCCESS'):
    return G + str(msg) + E

def WARN(msg='WARNING'):
    return Y + str(msg) + E

def TEXT(msg=''):
    return B + f"{msg}" + E

def TEXT_BOLD(msg=''):
    return BOLD + f"{msg}" + E

def status(idx, n):
    pc = round((idx+1)/n*100, 2)
    if int(pc)*1.0 == pc:
        pc = int(pc)
    return f'({pc}%, {idx+1}/{n})'

def write_log(file, text='', mode ='a'):
    with open(f'{file}', mode, encoding='utf-8') as f:
        f.write(str(text) + '\n')
        f.close()
        
class MySql():
    def __init__(self, host=None, user=None, port=None, password=None, database=None):
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.database = database
        self.auth_plugin = 'mysql_native_password'
        self.db = None
        self.cursor = None
        self.cols = None
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
        df = df[self.cols]
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
        self.cols = cols
        if src:
            data = self.get_data(src)
        sql_insert = f"insert into {table} values ({','.join(['%s']*len(cols))})" #  {table} ({','.join(cols)})
        print(sql_insert)
        loop = tqdm(range(len(data)), desc=f"Inserting {TEXT_BOLD(table)}...", colour='cyan', ncols=100)
        misses = 0
        for l in loop:
            try:
                # print(data[l])
                self.cursor.execute(sql_insert, data[l])
                loop.set_postfix(status=f'{OK("success")}')
                write_log(f'./dags/log/{table}.log', f'success')
            except Exception as e:
                write_log(f'./dags/log/{table}.log', f'fail: {e}')
                loop.set_postfix(status=f'{FAIL("fail")}: {e}')
                misses += 1
                continue
        print(OK(f"Done push data to {table}!") if misses!=len(data) else FAIL(f"Not push data to{table}!"))
        self.db.commit()
