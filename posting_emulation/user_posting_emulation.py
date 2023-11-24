'''
User posting emulation script for Pinterest Data Pipeline project (AiCore).
Runs an infinite loop to select one record of Pinterest data at a time
from RDS, and post to Kafka topics.

Author: Kristina Gorkovskaya
Date: 2023-11-10
'''

import json
import random
import requests
from posting_emulation_classes import PostingEmulation


class PostingEmulationMSK(PostingEmulation):
    '''
    This class connects to an AWS RDS database containing Pinterest data, then runs an 
    infinite loop to select one random row at a time from specified RDS tables and and post each row to 
    the appropriate Kafka topic for that table, via a REST API.
    '''

    def __init__(self):
        '''See help(PostingEmulationMSK) for accurate signature.
        Attributes:
            db_connector (AWSDBConnector): connection to Pinterest database
            invoke_url (str): partial API endpoint URL
            table_mapping (dict): maps table names to aliases which will be used to construct the API endpoint URL 
            user_id (str): AWS IAM User ID; this will be used to construct the API endpoint URL
        '''

        super().__init__()
        self.user_id = '0ec858bf1407'
        self.invoke_url = 'https://broydqcmtd.execute-api.us-east-1.amazonaws.com/test/topics/{}.{}'
        
                        
    def post(self, payload: dict, table_alias: str) -> None:
        '''Post a single record (row of data) to the specified Kafka topic via REST API.'''

        # Get endpoint URL for the specified topic
        endpoint_url = self.invoke_url.format(self.user_id, table_alias)
        
        # Serialize the data
        payload = json.dumps({"records": [{"value": payload}]})

        # Make POST request
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json', 'Accept': 'application/vnd.kafka.v2+json'}
        r = requests.request("POST", endpoint_url, headers=headers, data=payload)
        assert r.status_code == 200, f'Status code = {r.status_code}'



if __name__ == "__main__":
    p = PostingEmulationMSK()
    p.run_infinite_post_data_loop()

    
    


