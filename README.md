# Project: Data Pipelines with Airflow
The project is a part of Udacity Data Engineering Nanodegree, details can be found here: https://www.udacity.com/course/data-engineer-nanodegree--nd027

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

The project aims to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Data quality plays a big part when analyses are executed on top the data warehouse so the pipeline also run tests against the datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

Keywords: Apache Airflow, Apache Spark, Amazon Redshift, Amazon S3, PostgreSQL, Python
