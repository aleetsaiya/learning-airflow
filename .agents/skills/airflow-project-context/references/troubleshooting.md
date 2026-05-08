# Troubleshooting Reference

Use this reference for common issues in the Airflow/S3/Snowflake learning project.

## Docker Or Astro Is Not Running

Symptoms:

```text
We couldn't start the docker engine automatically.
no virtual environment found
```

Fix:

- Start Docker Desktop.
- Run `astro dev start`.
- If dependencies changed, run `astro dev restart`.

## Missing Airflow Connection

Symptoms:

```text
Connection not found: aws_s3
Connection not found: snowflake_default
```

Fix:

- Recreate Airflow connections after containers are reset.
- `store_raw_data` uses `aws_s3`.
- `load_to_snowflake` and `transform_in_snowflake` use `snowflake_default`.

## Missing Environment Variable

Symptoms:

```text
S3_RAW_BUCKET must be set.
AWS_ACCESS_KEY_ID must be set.
AWS_SECRET_ACCESS_KEY must be set.
```

Fix:

- Add required values to `.env`.
- Restart Astro so containers see the updated environment.

## Snowflake Account Identifier Error

Symptoms:

```text
404 Not Found
<wrong-account>.snowflakecomputing.com/session/v1/login-request
```

Likely cause:

- The Snowflake `account` value in Airflow connection Extra JSON is wrong.
- It should not be the username.

For newer Snowflake URLs:

```text
https://app.snowflake.com/<org_name>/<account_name>/...
```

use:

```text
<org_name>-<account_name>
```

## Snowflake Role Error

Symptoms:

```text
Requested role 'AIRFLOW_ROLE' is not assigned to the executing user.
```

Fix:

```sql
USE ROLE ACCOUNTADMIN;
GRANT ROLE AIRFLOW_ROLE TO USER <user_name>;
```

## Database Does Not Exist Or Not Authorized

Symptoms:

```text
Database 'LEARNING_AIRFLOW' does not exist or not authorized.
```

Fix:

- Make sure the database exists.
- Make sure the Airflow role has usage on the database.

```sql
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS LEARNING_AIRFLOW;
GRANT USAGE ON DATABASE LEARNING_AIRFLOW TO ROLE AIRFLOW_ROLE;
GRANT CREATE SCHEMA ON DATABASE LEARNING_AIRFLOW TO ROLE AIRFLOW_ROLE;
```

## Duplicate Raw Loads

Repeated `COPY INTO` runs can produce duplicate raw rows depending on Snowflake load history and rerun behavior. The analytics view uses deduplicating CTEs to avoid inflated post/comment counts.
