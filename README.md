
# Pinterest Data Pipieline

## Table of contents
1. [Description](#description)
1. [Installation instructions](#installation-instructions)
1. [Usage instructions](#usage-instructions)
1. [File structure](#file-structure)
1. [License information](#license-information)

## Description

Pinterest Data Pipeline project for AiCore. This project aims to recreate the data analytics used by Pinterest, using AWS Cloud tools. 

## Installation instructions
- Set up a Kafka client machine. Connect to an EC2 instance and install Kafka. Set up IAM authentication on the EC2 instance to ensure MSK can authenticate the EC2 instance (install the IAM MSK authentication package; set CLASSPATH to store the location of package .jar file; modify the client.properties file on the EC2 instance to configure the EC2 instance to use AWS IAM authentication to the cluster).
- Create Kafka topics on the client machine (the EC2 instance created in the previous step): 
    - <user_id>.pin: contains data about Pinterest posts
    - <user_id>.geo: contains data about geolocation of each post
    - <user_id>.user: contains data about users that uploaded each post
- Connect the MSK cluster to an S3 bucket. Use a sink connector, so any data going through the cluster will be automatically written to the S3 bucket. 
    - First create a custom plugin via the MSK Connect console (use the Confluent.io Amazon S3 connector). 
    - Then create a connector with the custom plugin; point it to the desired S3 bucket. Any data passing through the IAM authenticated cluster should now be written to the bucket.

## Usage instructions
TBC

## File structure
TBC

## License information