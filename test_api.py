#%%
import requests
from time import sleep
import random
from multiprocessing import Process
#import boto3
import json
import sqlalchemy
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


#%% 

invoke_url = 'https://broydqcmtd.execute-api.us-east-1.amazonaws.com/test/topics/0ec858bf1407.user'
headers = {
            'Content-Type': 'application/vnd.kafka.json.v2+json',
            'Accept': 'application/vnd.kafka.v2+json'
        }
    
num_rows = random.randint(0, 11000)
sql = text(f'SELECT * from user_data LIMIT {num_rows}, 1')

# Select one row
cxn = AWSDBConnector()
engine = cxn.create_db_connector()
with engine.connect() as connection:
    selected_rows = connection.execute(sql)     

    for row in selected_rows:
        values = dict(row._mapping)              

print(row)
print('\n')
print(row._mapping)
print('\n')
values['date_joined'] = values['date_joined'].strftime('%Y-%m-%d %H:%M:%S')

#%%
# Post to API
payload = json.dumps({"records": [{"value": values}]})
r = requests.request("POST", invoke_url, headers=headers, data=payload)
print(f'\nStatus code = {r.status_code}')



    
    



# %%
