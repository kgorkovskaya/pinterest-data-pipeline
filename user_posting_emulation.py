import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime


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


class PostingEmulation:

    topic_mapping = {
                    'pinterest_data': 'pin', 
                    'geolocation_data': 'geo',
                    'user_data': 'user'
                    }

    def __init__(self):
        self.user_id = '0ec858bf1407'
        self.db_connector = AWSDBConnector()
        self.invoke_url = 'https://broydqcmtd.execute-api.us-east-1.amazonaws.com/test/topics/'
        self.headers = {'Content-Type': 'application/vnd.kafka.json.v2+json',
                        'Accept': 'application/vnd.kafka.v2+json'
        }
                        

    def post(self, topic, record):
        
        # Convert datetime to string to ensure data can be serialized
        print(record)
        for k, v in record.items():
            if type(v) == datetime:
                record[k] = v.strftime('%Y-%m-%d %H:%M:%S')

        payload = json.dumps({"records": [{"value": record}]})
        url = self.invoke_url + self.user_id + '.' + topic
        r = requests.request("POST", url, headers=self.headers, data=payload)
        assert r.status_code == 200, f'Status code = {r.status_code}'



    def run_infinite_post_data_loop(self):
        engine = self.db_connector.create_db_connector()
        with engine.connect() as connection:
            while True:
                sleep(random.randrange(0, 2))
                random_number = random.randint(0, 11000)

                for table, topic in self.topic_mapping.items():
                    sql = text(f'SELECT * from {table} LIMIT {random_number}, 1')
                    selected_rows = connection.execute(sql)
                    for row in selected_rows:
                        values = dict(row._mapping)
                    self.post(topic, values)


if __name__ == "__main__":
    p = PostingEmulation()
    p.run_infinite_post_data_loop()

    
    


