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
 
 In my .bashrc file I set up the following environment variables:
 
 ```
 AWS_REGION="us-west-1"

AWS_ATHENA_KEY_ID=<my athena_user security credential ID>
AWS_ATHENA_ACCESS_KEY=<my athena-user secrect key> 

export AWS_REGION
export AWS_ATHENA_KEY_ID
export AWS_ATHENA_ACCESS_KEY
```

## Amazon Partitions

Athena scans all of the data, for every query. This many not be an issue for ORC files built from S3 web log data because the compression provided by the ORC file format results in every compact data. Due to the relatively small size of the S3 web log data in ORC format, the queries in this repository do not use partitioning.

The amount of data scanned by an Athena query can be reduced by using partitioned data, which can be important for large amounts of data.  An Athena data partition is a prefix in the S3 file path. 


I support several web sites via S3 static hosting. The [S3 Log Reader](https://github.com/IanLKaplan/s3logreader) allows the ORC data generated from the S3 web log data to be written to the ORC log file bucket.  To support partitioning a domain name prefix is included in the S3 file key (path) (e.g., bearcave.com and topstonesoftware.com).  This is shown below:

```
s3://ianlkaplan-logs.orc/user/iank/http_logs/bearcave.com/
s3://ianlkaplan-logs.orc/user/iank/http_logs/topstonesoftware.com/
```

Athena queries will traverse all sub-directories.  For a table created with the following DDL, all of the ORC data will be searched.

```
create external table `orclogdb.httplogs` (
    `bucket_name` string,
    `request_date` timestamp,
    `remote_ip` string,
    `operation` string,
    `key` string,
    `request_uri` string,
    `http_status` int,
    `total_time` int,
    `referrer` string,
    `user_agent` string,
    `version_id` string,
    `end_point` string
 )
stored as ORC
location 's3://ianlkaplan-logs.orc/user/iank/http_logs'
tblproperties ("orc.compress"="ZLIB")

```

The queries in this repository the domain subset of the data with the where clause ```bucket_name = <domain name>```, for example ```bucket_name = 'bearcave.com'```

A partition can be added in two steps:

1. Load the following DDL

```
create external table `orclogdb.httplogs` (
    `bucket_name` string,
    `request_date` timestamp,
    `remote_ip` string,
    `operation` string,
    `key` string,
    `request_uri` string,
    `http_status` int,
    `total_time` int,
    `referrer` string,
    `user_agent` string,
    `version_id` string,
    `end_point` string
 )
partitioned by (domain string)
stored as ORC
location 's3://ianlkaplan-logs.orc/user/iank/http_logs'
tblproperties ("orc.compress"="ZLIB")
```

This creates a partition, but this partition is not associated with a path prefix and a query against this table will not return any data.

2. Associate the partition with S3 path prefixes using an add partition statement.

```
alter table orclogdb.httplogs add partition(domain='topstonesoftware.com') location 's3://ianlkaplan-logs.orc/user/iank/http_logs/topstonesoftware.com/'
partition(domain="bearcave.com") location 's3://ianlkaplan-logs.orc/user/iank/http_logs/bearcave.com/';
 ```
 
 The following query will only return data for the ```bearcave.com``` partition and will only scan the data assocaited with this partition.
 
 ```
select * from orclogdb.httplogs
where domain = 'bearcave.com'
limit 10;
 ```
 
 Two steps are used here because this makes programatic partitions creation easier.  First the DDL without a partition can be loaded. Then the data can be queries to find the domains.  With the domain list the DDL with the named blank partition can be loaded.  The partitions can then be added using the ```alter table``` statement.
 
 Unlike relational databases, where loading data into a table can be expensive in time and processing resources, there is not cost to dropping and loading Athena tables since all data is stored in the files that Athena scans.
 
 
