'''
Classes for use in User Posting Emulation scripts, for Pinterest Data Pipeline project (AiCore).
Author: Kristina Gorkovskaya
Date: 2023-11-24
'''


import random
import sqlalchemy
import sys
import yaml
from abc import ABC, abstractmethod
from datetime import datetime
from sqlalchemy import text
from time import sleep


random.seed(100)


class AWSDBConnector:
    '''
    This class is used to connect to an RDS database on AWS.
    Attributes:
        HOST (str): host URL
        DATABASE (str): name of database
        PORT (int): port on which database in hosted
        USER (str): user name for database connection
        PASSWORD (str): password for database connection
    '''

    def __init__(self):
        '''Read RDS credentials from YAML file.'''

        try:
            with open('credentials_rds.yml', 'r') as f:
                credentials = yaml.safe_load(f) 

            self.HOST = credentials['host']
            self.DATABASE = credentials['database']
            self.PORT = credentials['port']
            self.USER = credentials['user']
            self.PASSWORD = credentials['password']

        except Exception as err:
            print(err)
            _ = input('Press enter to quit.')
            sys.exit()

        
    def create_db_connector(self) -> sqlalchemy.engine.base.Engine:
        '''Establish a connection to the RDS database.'''

        cxn_string = f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        engine = sqlalchemy.create_engine(cxn_string)
        return engine


class PostingEmulation(ABC):
    '''
    This Abstract Base Class connects to an AWS RDS database containing Pinterest data, then runs an 
    infinite loop to select one random row at a time from specified RDS tables and post each row to a REST API. 
    Attributes:
        db_connector (AWSDBConnector): connection to Pinterest database
        invoke_url (str): partial API endpoint URL
        table_mapping (dict): maps table names to aliases which will be used to construct the API endpoint URL 
        user_id (str): AWS IAM User ID; this will be used to construct the API endpoint URL
    '''

    def __init__(self):
        '''See help(PostingEmulation) for accurate signature.'''

        self.db_connector = AWSDBConnector() 
        self.user_id = None
        self.invoke_url = None
        self.table_mapping = {'pinterest_data': 'pin', 
                              'geolocation_data': 'geo',
                              'user_data': 'user'}


    @abstractmethod    
    def post(self, payload: dict, table_alias: str) -> None:
        '''Post a single record (row of data) to the specified API endpoint.
        Attributes:
            payload (dict): payload (dict): row of data to be posted (dictionary keys = column names)
            table_alias (str): alias of target table (this will be used to construct the endpoint URL)
        '''

        raise NotImplementedError
    

    @staticmethod
    def clean_payload(payload: dict) -> dict:
        '''Prepare data for posting; format datetimes as string to ensure data can be serialized.
        Attributes:
            payload (dict): row of data to be posted (dictionary keys = column names)
        '''

        print(payload)
        for k, v in payload.items():
            if type(v) == datetime:
                payload[k] = v.strftime('%Y-%m-%d %H:%M:%S')
        return payload


    def run_infinite_post_data_loop(self) -> None:
        '''Run infinite loop to select a random row from each in-scope RDS table, then post that row to the API.'''
        
        engine = self.db_connector.create_db_connector()
        with engine.connect() as connection:
            while True:
                sleep(random.randrange(0, 2))
                random_number = random.randint(0, 11000)

                for table, table_alias in self.table_mapping.items():

                    # Select random number of rows
                    sql = text(f'SELECT * from {table} LIMIT {random_number}, 1')
                    selected_rows = connection.execute(sql)

                    # Iterate over selected rows; convert the last one to a dict
                    # (keys = column names, values = datapoints).
                    # We iterate here because selected_rows is a sqlalchemy.engine.cursor.CursorResult
                    # object and is not subscriptable.
                    for row in selected_rows:
                        pass
                    
                    payload = dict(row._mapping)
                    payload = self.clean_payload(payload)
                    self.post(payload, table_alias)
