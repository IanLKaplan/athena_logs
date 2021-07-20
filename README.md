# athena_logs

## Introduction
This repository contains Java code that queries an Amazon Web Services (AWS) Athena database consisting of S3 web log data.

The AWS Athena database is based on the [Presto database](https://prestodb.io/) originally developed by Facebook. Athena supports ANSI SQL.  Unlike the AWS RDS databases, Athena usage is billed only when it is used. The free tier for Athena/AWS Glue allows 1 million free queries. This means that for processing S3 web log data, athena queries are generally free.

Athena can process raw S3 web log data, but the queries are slow because there are a large number of log files and processing the log text is slow.  By converting the log files to ORC format, no conversion is necessary and far less data must be scanned for each query.  The Java code in [S3 Log Reader](https://github.com/IanLKaplan/s3logreader) will read S3 web log files and build ORC files. The Java code in this repository relies on S3 web log data in ORC format.

## Analyzing S3 Web Log Data

AWS S3 static web hosting is one of the cheapest ways to host a web site.  Services like the AWS Elastic Search Service can be used to analyze S3 web log data (see [Analyzing Amazon S3 server access logs using Amazon ES](https://aws.amazon.com/blogs/big-data/analyzing-amazon-s3-server-access-logs-using-amazon-es)).  The AWS free tier provides 750 hours/month or 31.25 days of free usage for t2.small.elasticsearch or t3.small.elasticsearch instancess. AWS Elastic Search can be combined with AWS Lambda and API Gateway to create a web application (see [Creating a search application with Amazon Elasticsearch Service](https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/search-example.html))

An alternative is to use Athena and SQL queries in place of Elastic Search. This repository contains a number of SQL queries that could be used by an AWS Lambda backend for an S3 web log analysis application.

## IAM Permissions

Following the AWS practice of providing the minimal permissions needed for the applicaiton, I created a user in AWS Identity and Access Management (IAM): ```athena_user```.  I added three AWS managed policies to this user: 
```
 AmazonS3FullAccess

 AmazonAthenaFullAccess

 AWSLakeFormationDataAdmin
 ```
 These permissions allow access to the S3 ORC data, Athena access and the ability to create databases.
 
 ```
 AWS_REGION="us-west-1"

AWS_ATHENA_KEY_ID=<my athena_user security credential ID>
AWS_ATHENA_ACCESS_KEY=<my athena-user secrect key> 

export AWS_REGION
export AWS_ATHENA_KEY_ID
export AWS_ATHENA_ACCESS_KEY
```

