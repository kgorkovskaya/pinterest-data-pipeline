'''
User posting emulation script for Pinterest Data Pipeline project (AiCore).
Runs an infinite loop to select one record of Pinterest data at a time
from RDS, and post to Kinesis Data Streams.

Author: Kristina Gorkovskaya
Date: 2023-11-24
'''

import json
import random
import requests
import string
from posting_emulation_classes import PostingEmulation


class PostingEmulationKinesis(PostingEmulation):
    '''
    This class connects to an AWS RDS database containing Pinterest data, then runs an 
    infinite loop to select one random row at a time from specified RDS tables and post each row to 
    the appropriate Kinesis Data Stream for that table, via a REST API.
    Attributes:
        db_connector (AWSDBConnector): connection to Pinterest database
        invoke_url (str): partial API endpoint URL
        table_mapping (dict): maps table names to aliases which will be used to construct the API endpoint URL 
        user_id (str): AWS IAM User ID; this will be used to construct the API endpoint URL
        num_partitions (int): number of shards in the stream; determines how data will be split when posting.
    '''

    def __init__(self, num_shards: int = 4):
        '''See help(PostingEmulationKinesis) for accurate signature.'''

        super().__init__()
        self.user_id = '0ec858bf1407'
        self.invoke_url = 'https://broydqcmtd.execute-api.us-east-1.amazonaws.com/test/streams/record?stream-name=streaming-{}-{}'
        self.num_partitions = max(num_shards, 1)


    def post(self, payload: dict, table_alias: str) -> None:
        '''Post a single record (row of data) to the specified Kinesis Data String via REST API.
        Attributes:
            payload (dict): payload (dict): row of data to be posted (dictionary keys = column names)
            table_alias (str): alias of target table (this will be used to construct the endpoint URL)
        '''

        # Get endpoint URL for the specified topic
        endpoint_url = self.invoke_url.format(self.user_id, table_alias)

        # Assign a random partition key. Kinesis hashes this to determine
        # which shard to write to; all records with the same partition key
        # will be written to the same shard
        partition_key = ''.join([random.choice(string.ascii_lowercase) for _ in range(self.num_partitions)])
        
        # Serialize the data
        stream_name= f'streaming-{self.user_id}-{table_alias}'
        payload = json.dumps({
            'StreamName': stream_name, 
            'Data': {k:v for k, v in payload.items()}, 
            'PartitionKey': partition_key
            })

        # Make POST request
        headers = {'Content-Type': 'application/json'}
        r = requests.request('PUT', endpoint_url, headers=headers, data=payload)
        assert r.status_code == 200, f'Status code = {r.status_code}'


if __name__ == "__main__":
    p = PostingEmulationKinesis()
    p.run_infinite_post_data_loop()
