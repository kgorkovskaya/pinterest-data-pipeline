'''
AiCore Pinterest Data Pipeline Project

Mount S3 bucket to Databricks. 
This code is intended to run in a Databricks notebook.

Author: Kristina Gorkovskaya
Date: 2023-09-30
'''

from pyspark.sql.functions import *
import urllib

file_type = "csv"
first_row_is_header = "true"
delimiter = ","

# Read credentials (Access Key and Secret Access Key) from CSV
aws_keys_df = spark.read.format(file_type).option("header", first_row_is_header).option("sep", delimiter).load("/FileStore/tables/authentication_credentials.csv")

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']

# Encode the secret key for security purposes; 
# safe="" means every char will be encoded
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0ec858bf1407-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/0ec858bf1407"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)