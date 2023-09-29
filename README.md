
# Pinterest Data Pipieline

## Table of contents
1. [Description](#description)
1. [Installation instructions](#installation-instructions)
1. [Usage instructions](#usage-instructions)
1. [File structure](#file-structure)
1. [License information](#license-information)

## Description

This project is the Pinterest Data Pipeline for AiCore, aiming to replicate Pinterest's data analytics infrastructure using AWS Cloud tools.

## Installation Instructions

To set up the Pinterest Data Pipeline, follow these steps:

1. Set up a Kafka client machine:
    - Connect to an EC2 instance and install Kafka on the EC2 instance.
    - Configure IAM authentication on the EC2 instance. This will enable MSK (Managed Streaming for Kafka) to authenticate the client machine. Here's how:
        - Install the IAM MSK authentication package.
        - Set the CLASSPATH environment variable to include the location of the package's .jar file.
        - Modify the client.properties file, to configure it for AWS IAM authentication to the MSK cluster (set awsRoleArn to the relevant IAM access role)
        - Edit the trust policy of the IAM access role to enable authentication (add a principal; principal type = "IAM roles", ARN = ARN of access role)
    - Create the following Kafka Topics on the client machine:
        - <user_id>.pin: Contains data about Pinterest posts.
        - <user_id>.geo: Contains data about the geolocation of each post.
        - <user_id>.user: Contains data about the users who uploaded each post.
1. Connect the Pinterest MSK Cluster to an S3 Bucket:
    - Create a custom plugin via the MSK Connect console, using the Confluent.io Amazon S3 connector. 
    - Download the Confluent.io connector to the EC2 client machine created in the previous step; then copy the file to the relevant S3 bucket. Then open the MSK Console, set up a connector with the custom plugin and configure it to point to the desired S3 bucket.
    - Any data passing through the IAM-authenticated cluster should now be automatically written to the specified S3 bucket.
1. Create a REST API and integrate the API with the MSK cluster. This will enable the API to send data to the cluster. 
    - Create a REST API on AWS API Gateway. Add a new resource to the REST API and configure it as a proxy resource by using the following configuration:
        - Resource name = proxy
        - Resource path = /{proxy+}
        - Select "Enable API Gateway CORS"
    - Create a HTTP ANY method for the resource. Set the Endpoint URL to the PublicDNS of the EC2 client machine.
    - Deploy the API and make a note of the Invoke URL. 
1. Set up the Kafka REST Proxy on the EC2 client machine, to enable the API to communicate with the MSK cluster.
    - Install the Confluent package for the Kafka REST Proxy.
    - Allow the REST Proxy to perform IAM authentication to the MSK cluster by modifying the kafka-rest.properties file. Modify the bootstrap.servers and the zookeeper.connect variables in this file with the Bootstrap server string and Plaintext Apache Zookeeper connection string for the MSK cluster. 
1. Send data to the API.
    - To start the REST proxy on the EC2 client machine, navigate to the confluent-7.2.0/bin folder and run the following command:
    __./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties__
    - Execute user_posting_emulation.py locally; this connects to an RDS database containing Pinterest data, selects a random row from the pinterest_data, geolocation_data, and user_data tables, and sends a POST request to the API Invoke URL for the <user_id>.pin, <user_id>.geo, and <user_id>.user Kafka topics, respectively.
    - To check that data is being sent to the cluster, open one terminal window for each of the above topics and run a Kafka consumer in each window. To run a consumer, navigate to <your_kafka_directory>/bin, and run the following command:
    __./kafka-console-consumer.sh --bootstrap-server <bootstrap server string> --consumer.config client.properties --topic <topic_name> --from-beginning --group students__
    - If everything has been set up correctly, you should see messages being consumed.
    - Check if data is getting stored in the S3 bucket. 




## Usage instructions
TBC

## File structure
TBC

## License information