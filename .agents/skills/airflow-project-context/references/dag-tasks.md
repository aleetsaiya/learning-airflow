# DAG Tasks Reference

The main DAG is `dags/jsonplaceholder_etl.py`.

## DAG Flow

```text
fetch_posts
fetch_comments
fetch_users
  -> store_raw_data
  -> load_to_snowflake
  -> transform_in_snowflake
```

The three fetch tasks run in parallel because the source datasets are independent.

## Tasks 1-3: Fetch Source Data

Tasks:

- `fetch_posts`
- `fetch_comments`
- `fetch_users`

Each task calls `_fetch_endpoint()` with one JSONPlaceholder endpoint:

```text
/posts
/comments
/users
```

Each task returns a JSON list. In Airflow TaskFlow API, returned values are stored in XCom and passed to downstream tasks as function arguments.

## Task 4: Store Raw Data

Task: `store_raw_data`

Purpose:

- receives posts, comments, and users from XCom
- writes each dataset as JSON to Amazon S3
- uses Airflow `S3Hook`
- uses Airflow connection `aws_s3`

S3 path pattern:

```text
raw/posts/ds=<YYYY-MM-DD>/posts.json
raw/comments/ds=<YYYY-MM-DD>/comments.json
raw/users/ds=<YYYY-MM-DD>/users.json
```

`ds` is Airflow's logical date string for the DAG run.

## Task 5: Load To Snowflake

Task: `load_to_snowflake`

Purpose:

- connects to Snowflake with `SnowflakeHook`
- uses Airflow connection `snowflake_default`
- creates required Snowflake objects if they do not exist
- loads S3 JSON files into Snowflake raw tables with `COPY INTO`

Created objects:

```text
RAW schema
RAW.JSON_FORMAT
RAW.JSONPLACEHOLDER_RAW_STAGE
RAW.POSTS
RAW.COMMENTS
RAW.USERS
```

Raw tables store:

```text
record VARIANT
source_file STRING
loaded_at TIMESTAMP_LTZ
```

## Task 6: Transform In Snowflake

Task: `transform_in_snowflake`

Purpose:

- creates `ANALYTICS` schema if needed
- creates or replaces `ANALYTICS.POST_COMMENT_SUMMARY`
- extracts typed fields from Snowflake `VARIANT` JSON records
- joins posts, users, and comments
- counts comments per post

View columns:

```text
post_id
post_title
user_id
user_name
user_email
comment_count
```

The transform uses deduplicating CTEs so repeated raw loads do not inflate counts.
