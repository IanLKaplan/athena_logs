# athena_logs

## Introduction
This repository contains Java code that queries an Amazon Web Services (AWS) Athena database consisting of S3 web log data.

The AWS Athena database is based on the [Presto database](https://prestodb.io/) originally developed by Facebook. Athena supports ANSI SQL.  Unlike the AWS RDS databases, Athena usage is billed only when it is used. The free tier for Athena/AWS Glue allows 1 million free queries. This means that for processing S3 web log data, athena queries are generally free.

Athena can process raw S3 web log data, but the queries are slow because there are a large number of log files and processing the log text is slow.  By converting the log files to ORC format, no conversion is necessary and far less data must be scanned for each query.  The Java code in [S3 Log Reader](https://github.com/IanLKaplan/s3logreader) will read S3 web log files and build ORC files.
