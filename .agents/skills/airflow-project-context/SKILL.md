---
name: airflow-project-context
description: Use when answering questions about this repository's Apache Airflow learning project, including its DAG architecture, JSONPlaceholder source data, Amazon S3 storage, Snowflake loading/transformation, local setup, or common troubleshooting.
---

# Airflow Project Context

Use this skill to help a new agent quickly understand and explain this repository's Airflow/S3/Snowflake learning pipeline.

This skill provides static project context only. It does not query live Airflow, Amazon S3, or Snowflake. If the user asks for live run status, task logs, bucket contents, or warehouse data, inspect available local files first and clearly say when live service access is required.

## Reference Selection

Read only the reference file needed for the user's question:

- Architecture or project overview: `references/architecture.md`
- DAG tasks, task purpose, or data flow: `references/dag-tasks.md`
- Local setup, Airflow connections, AWS/Snowflake config: `references/setup.md`
- Errors, debugging, or common issues: `references/troubleshooting.md`

For broad questions like "explain this project", read `architecture.md` and `dag-tasks.md`. Answer concisely using the architecture and task flow, and keep the main focus on data integration with Airflow, S3, and Snowflake.

## Response Guidance

- Use plain language and explain Airflow concepts briefly when helpful.
- Treat this as a learning project that simulates a real data pipeline.
- Do not expose or invent secrets, bucket names, passwords, or account identifiers.
- Prefer the current DAG flow:

```text
fetch_posts / fetch_comments / fetch_users -> store_raw_data -> load_to_snowflake -> transform_in_snowflake
```

- Mention that raw data lands in S3 first, then Snowflake `RAW` tables, then the `ANALYTICS.POST_COMMENT_SUMMARY` view.
