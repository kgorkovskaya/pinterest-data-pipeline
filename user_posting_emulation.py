'''
User posting emulation script for Pinterest Data Pipeline project (AiCore).
Runs an infinite loop to select one record of Pinterest data at a time
from RDS, and post to Kafka topics.

Author: Kristina Gorkovskaya
Date: 2023-10-11
'''

import json
import random
import requests
import sqlalchemy
import sys
import yaml
from datetime import datetime
from sqlalchemy import text
from time import sleep



random.seed(100)


class AWSDBConnector:
    '''
    This class is used to connect to an RDS database on AWS.
    Constructor method reads login credentials from YAML file (credentials_rds.yml)
	Attributes:
		HOST (str): host URL
        DATABASE (str): name of database
        PORT (int): port on which database in hosted
        USER (str): user name for database connection
        PASSWORD (str): password for database connection
	'''


    def __init__(self):
        '''Read login credentials from YAML file.'''

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

        
    def create_db_connector(self):
        '''
        Establish a connection to the RDS database.
        Returns:
            sqlalchemy.engine.base.Engine
        '''
        cxn_string = f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        engine = sqlalchemy.create_engine(cxn_string)
        return engine


class PostingEmulation:
    '''
    This class is used to connect to an AWS RDS database containing Pinterest data, then run an 
    infinite loop to select one random row at a time from specified tables (containing pins, user data,
    and geolocation data) and post each row to an associated Kafka topic. 

	Attributes:
		db_connector (AWSDBConnector): connection to Pinterest database
        headers (dict): headers for POST requests
        invoke_url(str): partial URL of API endpoint
        topic_mapping (dict): maps table names to partial topic names
        user_id (str): AWS IAM User ID; forms part of the full topic names 
            (topic names are in the format <user_id>.<suffix>)
	'''


    def __init__(self):
        '''
        Initialise variables and read credentials from YAML file (credentials_msk.yml)
        '''

        self.db_connector = AWSDBConnector()

        self.headers = {'Content-Type': 'application/vnd.kafka.json.v2+json',
                        'Accept': 'application/vnd.kafka.v2+json'}
        
        self.topic_mapping = {'pinterest_data': 'pin', 
                              'geolocation_data': 'geo',
                              'user_data': 'user'}
        
        try:
            with open('credentials_msk.yml', 'r') as f:
                credentials = yaml.safe_load(f) 
            self.user_id = credentials['user_id']
            self.invoke_url = credentials['invoke_url']

        except Exception as err:
            print(err)
            _ = input('Press enter to quit.')
            sys.exit()
                        

    def post(self, topic_name_suffix, record):
        '''
        Post a single record (row of data) to the specified Kafka topic via REST API.
        Args:
            topic_name_suffix (str): partial topic name
            record (dict): record being posted to Kafka. 
                keys = column names; values = datapoints
        Returns:
            None
        '''
        
        # Print the record to the console
        print(record)

        # Convert datetime values to string to ensure data can be serialized
        for k, v in record.items():
            if type(v) == datetime:
                record[k] = v.strftime('%Y-%m-%d %H:%M:%S')

        # Serialize the data
        payload = json.dumps({"records": [{"value": record}]})

        # Get endpoint URL for the specified topic
        url = self.invoke_url + self.user_id + '.' + topic_name_suffix

        # Make POST request
        r = requests.request("POST", url, headers=self.headers, data=payload)
        assert r.status_code == 200, f'Status code = {r.status_code}'



    def run_infinite_post_data_loop(self):
        '''
        Run infinite loop to select a random row from each in-scope 
        RDS table, then post that row to the relevant Kafka topic.
        Returns:
            None
        '''
        engine = self.db_connector.create_db_connector()
        with engine.connect() as connection:
            while True:
                sleep(random.randrange(0, 2))
                random_number = random.randint(0, 11000)

                for table, topic_name_suffix in self.topic_mapping.items():

                    # Select random number of rows
                    sql = text(f'SELECT * from {table} LIMIT {random_number}, 1')
                    selected_rows = connection.execute(sql)

                    # Iterate over the rows; convert the last one to a dict
                    # (keys = column names, values = datapoints).
                    # We iterate here bcause selected_rows is a sqlalchemy.engine.cursor.CursorResult
                    # object and is not subscriptable.
                    for row in selected_rows:
                        pass
                    
                    record = dict(row._mapping)
                    self.post(topic_name_suffix, record)


if __name__ == "__main__":
    p = PostingEmulation()
    p.run_infinite_post_data_loop()

    
    


