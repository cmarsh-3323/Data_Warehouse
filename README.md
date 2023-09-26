# Data Warehouse
Project submission for Udacity Data Engineering Nano Degree

Author: Chris Marshall


## Project Summary

The startup company 'Sparkify' is ready to move to the cloud! Sparkify is looking to hire a data engineer to build an ETL(extract,transform,load) pipeline using AWS (amazon web services). They need you to extract their data from S3 (simple storage service), stage it into a redshift cluster and transform the data for Sparkify's analytical purposes.

Sparkify has two datasets located within S3, song_data and log_data. Your objective is to pull this data from S3 and then structure a star schema by transforming the data into a set of dimensional tables.



## Getting Started and Usage
* Make sure to have Python installed on your system. You can download python from the official website: [Python Downloads](https://www.python.org/downloads/)

* Run command to install necessary requirements:

    `pip install -r requirements.txt` 

* Create the tables by running the following command:

   `python create_tables.py`

* Load data into staging tables and insert data from staging tables into final table by running the following command:

   `python etl.py`

## Directory Structure
This section provides an overview of the major files in the repository and their respective purposes and functionalities.

| File Name     | Purpose                                              |
|---------------|------------------------------------------------------|
| create_tables.py   | Manages the database star schema for our redshift cluster. Drops existing tables and creates new tables. |
| dwh.cfg   | Configuration file with credentials to AWS resources          |
| etl.py   |Performs ETL that loads data from S3 into staging tables within the redshift cluster and transforms data into tables on redshift for Sparkify's analytical goals.   |
| README.md     | Documentation for Project: Data Warehouse                       |
| sql_queries.py      |  Contains all of our SQL statements that are imported into etl.py and create_table.py   |

## References

[AWS Docs](https://docs.aws.amazon.com/)

[Redshift Create Table Docs](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)

[Redshift Cluster Guide](https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html)