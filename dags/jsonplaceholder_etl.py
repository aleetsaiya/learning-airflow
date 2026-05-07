"""Fetch JSONPlaceholder source data for a learning ETL pipeline."""

import json
import logging
import os
import re
from typing import Any

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task
from pendulum import datetime, duration


log = logging.getLogger("airflow.task")

DATASET_TABLES = {
    "posts": "POSTS",
    "comments": "COMMENTS",
    "users": "USERS",
}

JSONPLACEHOLDER_BASE_URL = os.getenv(
    "JSONPLACEHOLDER_BASE_URL", "https://jsonplaceholder.typicode.com"
).rstrip("/")

def _fetch_endpoint(endpoint: str) -> list[dict[str, Any]]:
    response = requests.get(f"{JSONPLACEHOLDER_BASE_URL}/{endpoint}", timeout=30)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise TypeError(f"Expected {endpoint} response to be a list.")

    log.info("Fetched %s %s records.", len(data), endpoint)
    return data


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} must be set.")
    return value


def _snowflake_string(value: str) -> str:
    return value.replace("'", "''")


@dag(
    dag_id="jsonplaceholder_etl",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": duration(seconds=30),
    },
    tags=["jsonplaceholder", "etl", "learning"],
)
def jsonplaceholder_etl():
    @task
    def fetch_posts() -> list[dict[str, Any]]:
        return _fetch_endpoint("posts")

    @task
    def fetch_comments() -> list[dict[str, Any]]:
        return _fetch_endpoint("comments")

    @task
    def fetch_users() -> list[dict[str, Any]]:
        return _fetch_endpoint("users")

    @task
    def store_raw_data(
        posts: list[dict[str, Any]],
        comments: list[dict[str, Any]],
        users: list[dict[str, Any]],
        **context: Any,
    ) -> None:
        bucket_name = _require_env("S3_RAW_BUCKET")
        aws_conn_id = "aws_s3"
        ds = context["ds"]

        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        datasets = {
            "posts": posts,
            "comments": comments,
            "users": users,
        }

        for dataset_name, dataset in datasets.items():
            key = f"raw/{dataset_name}/ds={ds}/{dataset_name}.json"
            s3_hook.load_string(
                string_data=json.dumps(dataset, indent=2),
                key=key,
                bucket_name=bucket_name,
                replace=True,
            )
            log.info(
                "Stored %s %s records at s3://%s/%s.",
                len(dataset),
                dataset_name,
                bucket_name,
                key,
            )

    @task
    def load_to_snowflake(**context: Any) -> None:
        # AWS config
        bucket_name = _require_env("S3_RAW_BUCKET")
        aws_access_key_id = _require_env("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = _require_env("AWS_SECRET_ACCESS_KEY")
        
        # Snowflake config
        snowflake_conn_id = "snowflake_default"
        raw_schema = "RAW"
        stage_name = "JSONPLACEHOLDER_RAW_STAGE"
        file_format_name =  "JSON_FORMAT"
        
        ds = context["ds"]

        stage_path = f"{raw_schema}.{stage_name}"
        file_format_path = f"{raw_schema}.{file_format_name}"
        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        # Create SCHEMA, STAGE, FILE_FORMAT, and TABLE before implement 'copy into' script
        sql_statements = [
            f"CREATE SCHEMA IF NOT EXISTS {raw_schema}",
            f"""
            CREATE FILE FORMAT IF NOT EXISTS {file_format_path}
            TYPE = JSON
            STRIP_OUTER_ARRAY = TRUE
            """,
            f"""
            CREATE STAGE IF NOT EXISTS {stage_path}
            URL = 's3://{_snowflake_string(bucket_name)}/raw/'
            CREDENTIALS = (
                AWS_KEY_ID = '{_snowflake_string(aws_access_key_id)}'
                AWS_SECRET_KEY = '{_snowflake_string(aws_secret_access_key)}'
            )
            FILE_FORMAT = {file_format_path}
            """,
        ]

        for table_name in DATASET_TABLES.values():
            sql_statements.append(
                f"""
                CREATE TABLE IF NOT EXISTS {raw_schema}.{table_name} (
                    record VARIANT,
                    source_file STRING,
                    loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """
            )

        # Copy data from S3 bucket to Snowflake table
        for dataset_name, table_name in DATASET_TABLES.items():
            sql_statements.append(
                f"""
                COPY INTO {raw_schema}.{table_name} (record, source_file)
                FROM (
                    SELECT $1, METADATA$FILENAME
                    FROM @{stage_path}/{dataset_name}/ds={ds}/{dataset_name}.json
                )
                FILE_FORMAT = (FORMAT_NAME = {file_format_path})
                """
            )

        snowflake_hook.run(sql_statements)
        log.info("Loaded JSONPlaceholder raw data for ds=%s into Snowflake.", ds)

    @task
    def transform_in_snowflake() -> None:
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        sql_statements = [
            "CREATE SCHEMA IF NOT EXISTS ANALYTICS",
            """
            CREATE OR REPLACE VIEW ANALYTICS.POST_COMMENT_SUMMARY AS
            WITH posts AS (
                SELECT DISTINCT
                    record:id::NUMBER AS post_id,
                    record:userId::NUMBER AS user_id,
                    record:title::STRING AS post_title
                FROM RAW.POSTS
            ),
            users AS (
                SELECT DISTINCT
                    record:id::NUMBER AS user_id,
                    record:name::STRING AS user_name,
                    record:email::STRING AS user_email
                FROM RAW.USERS
            ),
            comments AS (
                SELECT DISTINCT
                    record:id::NUMBER AS comment_id,
                    record:postId::NUMBER AS post_id
                FROM RAW.COMMENTS
            )
            SELECT
                posts.post_id,
                posts.post_title,
                posts.user_id,
                users.user_name,
                users.user_email,
                COUNT(comments.comment_id) AS comment_count
            FROM posts
            LEFT JOIN users
                ON posts.user_id = users.user_id
            LEFT JOIN comments
                ON posts.post_id = comments.post_id
            GROUP BY
                posts.post_id,
                posts.post_title,
                posts.user_id,
                users.user_name,
                users.user_email
            """,
        ]

        snowflake_hook.run(sql_statements)
        log.info("Created or replaced ANALYTICS.POST_COMMENT_SUMMARY view.")

    posts = fetch_posts()
    comments = fetch_comments()
    users = fetch_users()
    raw_data = store_raw_data(posts, comments, users)
    raw_data >> load_to_snowflake() >> transform_in_snowflake()


jsonplaceholder_etl()
