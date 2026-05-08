# Setup Reference

This project runs locally with Astro CLI and Docker.

## Local Airflow

Start Airflow:

```bash
astro dev start
```

Restart after changing `.env` or dependencies:

```bash
astro dev restart
```

Run DAG tests:

```bash
astro dev pytest
```

Airflow UI is usually:

```text
http://localhost:8080
```

## Dependencies

Important Python packages in `requirements.txt`:

```text
apache-airflow-providers-amazon
apache-airflow-providers-snowflake
pandas
requests
```

## Airflow Connections

Amazon S3 connection:

```text
Connection ID: aws_s3
Connection Type: Amazon Web Services
Login: <AWS access key ID>
Password: <AWS secret access key>
```

Extra JSON:

```json
{
  "region_name": "ap-southeast-2"
}
```

Snowflake connection:

```text
Connection ID: snowflake_default
Connection Type: Snowflake
Login: AIRFLOW_USER
Password: <AIRFLOW_USER password>
Schema: RAW
```

Extra JSON:

```json
{
  "account": "<your_snowflake_account_identifier>",
  "warehouse": "JSONPLACEHOLDER_WH",
  "database": "LEARNING_AIRFLOW",
  "role": "AIRFLOW_ROLE"
}
```

The Snowflake account identifier is not the username. For a URL like:

```text
https://app.snowflake.com/<org_name>/<account_name>/...
```

use:

```text
<org_name>-<account_name>
```

## Environment Variables

The DAG reads these values from `.env` inside the Astro containers:

```env
S3_RAW_BUCKET=<your-s3-bucket-name>
AWS_ACCESS_KEY_ID=<AWS access key ID>
AWS_SECRET_ACCESS_KEY=<AWS secret access key>
```

Do not commit `.env` or real secrets.

## Snowflake Objects

Expected learning setup:

```text
Warehouse: JSONPLACEHOLDER_WH
Database: LEARNING_AIRFLOW
Role: AIRFLOW_ROLE
User: AIRFLOW_USER
Schemas: RAW, ANALYTICS
```

Verify data after a successful DAG run:

```sql
USE WAREHOUSE JSONPLACEHOLDER_WH;
USE DATABASE LEARNING_AIRFLOW;
USE SCHEMA RAW;

SELECT COUNT(*) FROM POSTS;
SELECT COUNT(*) FROM COMMENTS;
SELECT COUNT(*) FROM USERS;
```

Expected JSONPlaceholder counts:

```text
POSTS: 100
COMMENTS: 500
USERS: 10
```
