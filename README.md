
# Pinterest Data Pipieline

## Table of contents
1. [Description](#description)
1. [Installation instructions](#installation-instructions)
1. [Usage instructions](#usage-instructions)
1. [File structure](#file-structure)
1. [License information](#license-information)

## Description

This project is the Pinterest Data Pipeline for AiCore, aiming to replicate Pinterest's data analytics infrastructure using AWS Cloud tools.

## Installation instructions
Description
This project is the Pinterest Data Pipeline for AiCore, aiming to replicate Pinterest's data analytics infrastructure using AWS Cloud tools.

## Installation Instructions

To set up the Pinterest Data Pipeline, follow these steps:

1. Set up a Kafka Client Machine:
1. Connect to an EC2 instance.
1. Install Kafka on the EC2 instance.
1. Configure IAM authentication on the EC2 instance to enable MSK (Managed Streaming for Kafka) to authenticate it. Here's how:
    - Install the IAM MSK authentication package.
    - Set the CLASSPATH environment variable to include the location of the package's .jar file
    - Modify the client.properties file on the EC2 instance to configure it for AWS IAM authentication to the MSK cluster.
1. Create Kafka Topics: On the client machine (EC2 instance created in the previous step), create the following Kafka topics:
    - <user_id>.pin: Contains data about Pinterest posts.
    - <user_id>.geo: Contains data about the geolocation of each post.
    -   <user_id>.user: Contains data about the users who uploaded each post.
1. Connect the MSK Cluster to an S3 Bucket:
    - Create a custom plugin via the MSK Connect console using the Confluent.io Amazon S3 connector.
    - Set up a connector with the custom plugin and configure it to point to the desired S3 bucket.
    -   With this configuration, any data passing through the IAM-authenticated cluster will be automatically written to the specified S3 bucket.

## Usage instructions
TBC

## File structure
TBC

## License information