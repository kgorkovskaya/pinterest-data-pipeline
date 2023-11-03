
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

    - Connect to an EC2 instance and install Kafka by running the following commands (be sure to install the same version of Kafka as the one the Pinterest MSK cluster is running on):
    <br>`sudo yum install java-1.8.0`
    <br>`wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz`
    <br>`tar -xzf kafka_2.12-2.8.1.tgz`

    - Configure IAM authentication on the EC2 instance. This will enable MSK (Managed Streaming for Kafka) to authenticate the client machine. Here's how:

        - Install the IAM MSK authentication package by running the following commands:
        <br>`cd kafka_2.12-2.8.1/libs`
        <br>`wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.5-all.jar`

        - Set the CLASSPATH environment variable to include the location of the package's .jar file, to ensure the IAM authentication libraries will be accessible to the Kafka client:
        <br>`export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar`

        - Navigate to the IAM console, select the EC2 access role associated with your IAM user, and copy its ARN. Go to the Trust relationships tab for the EC2 access role; select "Edit trust policy"; click "Add a principal"; select "IAM roles" as the Principal type.  Replace the ARN with the ARN you copied earlier.

        - Configure the Kafka client to use AWS IAM authentication to the cluster. To do this, modify the __client.properties__ file inside your __kafka_2.12-2.8.1/bin__ directory. Sample text is shown below; set awsRoleArn to point to the ARN of the EC2 access role from the previous step.

            >`# Sets up TLS for encryption and SASL for authN.`
            ><br>`security.protocol = SASL_SSL`
            ><br><br>`# Identifies the SASL mechanism to use.`
            ><br>`sasl.mechanism = AWS_MSK_IAM`
            ><br><br>`# Binds SASL client implementation.`
            ><br>`sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="<ARN_of_EC2_access_role>";`
            ><br><br>`# Encapsulates constructing a SigV4 signature based on extracted credentials.`
            ><br>`# The SASL client bound by "sasl.jaas.config" invokes this class.`
            ><br>`sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler`


1. Create the following Kafka Topics on the client machine:
    - __<user_id>.pin__: Contains data about Pinterest posts.
    - __<user_id>.geo__: Contains data about the geolocation of each post.
    - __<user_id>.user__: Contains data about the users who uploaded each post.
<br><br>To create the above topics, navigate to your __kafka_2.12-2.8.1/bin__ directory and run the following command, replacing the placeholders with the Bootstrap Server String of the Kafka client (EC2 instance), and the topic name:
<br>`./kafka-topics.sh --bootstrap-server <bootstrap_server_string> --command-config client.properties --create --topic <topic_name>`
    

1. Connect the Pinterest MSK Cluster to an S3 Bucket:

    - Download the Confluent.io connector to the EC2 client machine created in the previous step, then copy the file to the relevant S3 bucket. To do this, run the following commands (replace the <bucket_name> placeholder with the name of the S3 bucket associated with your IAM user ID):
    <br>`sudo -u ec2-user -i`
    <br>`mkdir kafka-connect-s3 && cd kafka-connect-s3`
    <br>`wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip`
    <br>`aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<bucket_name>/kafka-connect-s3/`

    - Create a custom plugin via the MSK Connect console. To do this, open the MSK console, navigate to "Customised Plugins", and click "Create Customised Plugin". Select the S3 bucket containing the zip file downloaded in the previous step. In the list of objects in the bucket, select the zip file. 

    - Then set up a connector with the custom plugin, and configure it to point to the desired S3 bucket. To do this, open the MSK console, navigate to "Connectors", select "Create Connector". When creating the connector, select the newly-created plugin and the relevant MSK cluster.

    - In the connector config settings, enter the following configuration, replacing the placholders with your IAM user ID and S3 bucket name. Set the s3.region to point to the same region as the bucket and cluster.
        > `connector.class=io.confluent.connect.s3.S3SinkConnector`
        > <br>`s3.region=us-east-1`
        > <br> `flush.size=1`
        > <br> `schema.compatibility=NONE
        tasks.max=3`
        > <br>`topics.regex=<user_id>.*`
        > <br>`format.class=io.confluent.connect.s3.format.json.JsonFormat`
        > <br>`partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner`
        > <br>`value.converter.schemas.enable=false`
        > <br>`value.converter=org.apache.kafka.connect.json.JsonConverter`
        > <br>`storage.class=io.confluent.connect.s3.storage.S3Storage`
        > <br>`key.converter=org.apache.kafka.connect.storage.StringConverter`
        > <br>`s3.bucket.name=<bucket_name>`

    - Any data passing through the IAM-authenticated cluster, where the topic matches the __topics.regex__ pattern, should now be automatically written to the specified S3 bucket.

1. Create a REST API and integrate the API with the Kafka client (EC2 instance), then set up the Kafka REST Proxy on the Kafka client. This will enable the API to send data to the cluster. 

    - Create a REST API on AWS API Gateway. Add a new resource to the REST API and configure it as a proxy resource by using the following configuration:
        - __Resource name = proxy__
        - __Resource path = /{proxy+}__
        - __Select "Enable API Gateway CORS"__
    - Create a HTTP ANY method for the resource. Set the Endpoint URL to the PublicDNS of the EC2 client machine.
    - Deploy the API and make a note of the Invoke URL. 
    - Install the Confluent package for the Kafka REST Proxy by running the following commands:
    <br>`sudo wget https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz`
    <br>`tar -xvzf confluent-7.2.0.tar.gz`
    - Allow the REST Proxy to perform IAM authentication to the MSK cluster by modifying the __kafka-rest.properties__ file (in __confluent-7.2.0/etc/kafka-rest__). Modify the bootstrap.servers and the zookeeper.connect variables with the Bootstrap server string and Plaintext Apache Zookeeper connection string __for the MSK cluster__. Set awsRoleArn to the ARN of the EC2 access role from the previous steps. Sample text is shown below.

        > `# Copyright 2018 Confluent Inc.`
        > <br>`# Licensed under the Confluent Community License (the "License"); you may not use`
        > <br>`# this file except in compliance with the License.  You may obtain a copy of the`
        > <br>`# License at`
        > <br>`#`
        > <br>`# http://www.confluent.io/confluent-community-license`
        > <br>`#`
        > <br>`# Unless required by applicable law or agreed to in writing, software`
        > <br>`# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT`
        > <br>`# WARRANTIES OF ANY KIND, either express or implied.  See the License for the`
        > <br>`# specific language governing permissions and limitations under the License.`
        > <br>`#`
        > <br><br>`#id=kafka-rest-test-server`
        > <br>`#schema.registry.url=http://localhost:8081`
        > <br>`zookeeper.connect=<zookeeper_connection_string>`
        > <br>`bootstrap.servers=<bootstrap_server_string>`
        > <br>`#`
        > <br>`# Configure interceptor classes for sending consumer and producer metrics to Confluent Control Center`
        > <br>`# Make sure that monitoring-interceptors-<version>.jar is on the Java class path`
        > <br>`#consumer.interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor`
        > <br>`#producer.interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor`
        > <br>`client.security.protocol = SASL_SSL`
        > <br><br>`# Identifies the SASL mechanism to use.`
        > <br>`client.sasl.mechanism = AWS_MSK_IAM`
        > <br><br>`# Binds SASL client implementation.`
        > <br>`client.sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required`
        > <br>`awsRoleArn="<ARN_of_EC2_access_role>"`
        > <br><br>`# Encapsulates constructing a SigV4 signature based on extracted credentials.`
        > <br>`# The SASL client bound by "sasl.jaas.config" invokes this class.`
        > <br>`client.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler`


1. Mount S3 bucket to Databricks.

    - Log into Databricks and mount the S3 bucket associated with your IAM user to Databricks. This will enable Databricks to read data from the S3 bucket. The student Databricks account has full access to S3, so in this instance there is no need to create a new Access Key and Secret Access Key for Databricks.

    - To mount the S3 bucket, follow these steps:
        - Run the notebook __mount_s3_bucket.ipynb__ in Databricksa; replacing AWS_S3_BUCKET with the bucket name relevant to your user ID, and MOUNT_NAME with a value of your choice. This will return True if the bucket was mounted successfully. You only need to mount the bucket once, and then you should be able to access it from Databricks at any time. 
        - Check if the bucket was mounted successfully. If inside the mounted S3 bucket your data is organised in folders, you can specify the whole path in the above command after /mnt/mount_name. With the correct path specified, you should be able to see the contents of the S3 bucket when running the above Python code in a Databricks notebook (replace the mount_name placeholder with the mount name you assigned to the S3 bucket in the previous step).
        <br>`display(dbutils.fs.ls("/mnt/<mount_name>/../.."))`

## Usage instructions

1. Send data to the API.

    - To start the REST proxy on the EC2 client machine, navigate to the __confluent-7.2.0/bin__ folder and run the following command:
    <br>`./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties`

    - Execute __user_posting_emulation.py__ locally; this connects to an RDS database containing Pinterest data, selects a random row from the pinterest_data, geolocation_data, and user_data tables, and sends POST requests to the API Invoke URLs for the <user_id>.pin, <user_id>.geo, and <user_id>.user Kafka topics, respectively. This is repeated continuously until the program is terminated.

    - To check that data is being sent to the cluster, open one terminal window for each of the above topics and run a Kafka consumer in each window. To run a consumer, navigate to __kafka_2.12-2.8.1/bin__, and execute the following command:
    <br>`./kafka-console-consumer.sh --bootstrap-server <bootstrap server string> --consumer.config client.properties --topic <topic_name> --from-beginning --group students`

    - If everything has been set up correctly, you should see messages being consumed.
    - Check if data is getting stored in the S3 bucket by inspecting the bucket via the AWS management console. 


1. Read and analyse data from S3 in Databricks.

    - Run the Python notebook __read_and_analyse.ipynb__ in Databricks, to load data from the mounted bucket into Pandas DataFrames, clean and analyse the data. The following analysis is performed:
    
        - Clean the DataFrame that contains information about Pinterest posts. Perform the following transformations:
            - Replace empty entries and entries with no relevant data in each column with Nones
            - Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.
            - Ensure that each column containing numeric data has a numeric data type
            - Clean the data in the save_location column to include only the save location path
            - Rename the index column to ind.
            - Reorder the DataFrame columns to have the following column order: ind, unique_id, title, description, follower_count, poster_name, tag_list, is_image_or_video, image_src, save_location, category
        
        - Clean the DataFrame that contains information about geolocation. Perform the following transformations:
            - Create a new column coordinates that contains an array based on the latitude and longitude columns.
            - Drop the latitude and longitude columns from the DataFrame.
            - Convert the timestamp column from a string to a timestamp data type.
            - Reorder the DataFrame columns to have the following column order: ind, country, coordinates, timestamp

        - Clean the DataFrame that contains information about users. Perform the following transformations:
            - Create a new column user_name that concatenates the information found in the first_name and last_name columns
            - Drop the first_name and last_name columns from the DataFrame
            - Convert the date_joined column from a string to a timestamp data type
            - Reorder the DataFrame columns to have the following column order: ind, user_name, age, date_joined




## File structure
TBC

## License information