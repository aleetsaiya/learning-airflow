# Architecture Reference

This repository is an Apache Airflow learning project for data integration.

## System Flow

```text
JSONPlaceholder API
  -> Airflow DAG
  -> Amazon S3 raw JSON files
  -> Snowflake RAW tables
  -> Snowflake ANALYTICS view
```

The mock data source is JSONPlaceholder:

```text
https://jsonplaceholder.typicode.com/
```

The project extracts:

- posts
- comments
- users

## Purpose

The project demonstrates how a simple real-world style data pipeline can be orchestrated by Airflow:

- Python API extraction
- Airflow TaskFlow API
- parallel upstream tasks
- task dependencies
- Airflow XCom output passing
- Amazon S3 raw data storage
- Snowflake raw loading
- Snowflake SQL transformation

## Storage And Compute Concepts

Amazon S3 stores raw JSON files under date-partitioned paths.

Snowflake stores loaded data in database objects. In Snowflake, a database stores schemas, tables, stages, file formats, and views. A warehouse is compute that runs SQL against those database objects.

The current Snowflake layout is:

```text
LEARNING_AIRFLOW
  RAW
    POSTS
    COMMENTS
    USERS
    JSON_FORMAT
    JSONPLACEHOLDER_RAW_STAGE
  ANALYTICS
    POST_COMMENT_SUMMARY
```
