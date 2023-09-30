'''
AiCore Pinterest Data Pipeline Project

Load data from mounted S3 bucket into Pandas DataFrames; clean and analyse the data.
This code is intended to run in a Databricks notebook.

Author: Kristina Gorkovskaya
Date: 2023-059-30
'''

import pandas as pd

user_id = '0ec858bf1407'
mount_name = user_id

def load_data(topic_suffix: str) -> pd.DataFrame:
	'''Load data from S3 bucket into Pandas DataFrame.'''

	file_location = f'/mnt/{mount_name}/topics/{user_id}.{topic_suffix}/partition=0/*.json'
	file_type = 'json'
	infer_schema = 'true'
	df = spark.read.format(file_type).option('inferSchema', infer_schema).load(file_location)
	display(df)

if __name__ == '__main__':

    df_pin = load_data('pin')
    df_geo = load_data('geo')
    df_user = load_data('user')
