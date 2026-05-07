"""DAG import tests for the local Airflow project."""

import logging
import os
from contextlib import contextmanager

import pytest
from airflow.models import DagBag

JSONPLACEHOLDER_DAG_ID = "jsonplaceholder_etl"
FETCH_TASK_IDS = {"fetch_posts", "fetch_comments", "fetch_users"}
JSONPLACEHOLDER_TASK_IDS = FETCH_TASK_IDS | {
    "store_raw_data",
    "load_to_snowflake",
    "transform_in_snowflake",
}


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """Generate import errors from the local DAG bag."""
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Every DAG file should import cleanly."""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


def get_dag(dag_id):
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)
    return dag_bag.get_dag(dag_id)


def test_jsonplaceholder_dag_exists():
    assert get_dag(JSONPLACEHOLDER_DAG_ID) is not None


def test_jsonplaceholder_dag_has_expected_tasks():
    dag = get_dag(JSONPLACEHOLDER_DAG_ID)

    assert set(dag.task_ids) == JSONPLACEHOLDER_TASK_IDS


def test_jsonplaceholder_fetch_tasks_flow_to_store_raw_data():
    dag = get_dag(JSONPLACEHOLDER_DAG_ID)

    for task_id in FETCH_TASK_IDS:
        assert dag.get_task(task_id).downstream_task_ids == {"store_raw_data"}


def test_jsonplaceholder_store_raw_data_flows_to_load_to_snowflake():
    dag = get_dag(JSONPLACEHOLDER_DAG_ID)

    assert dag.get_task("store_raw_data").downstream_task_ids == {"load_to_snowflake"}


def test_jsonplaceholder_load_to_snowflake_flows_to_transform_in_snowflake():
    dag = get_dag(JSONPLACEHOLDER_DAG_ID)

    assert dag.get_task("load_to_snowflake").downstream_task_ids == {
        "transform_in_snowflake"
    }


def test_jsonplaceholder_transform_in_snowflake_has_no_downstream_tasks():
    dag = get_dag(JSONPLACEHOLDER_DAG_ID)

    assert dag.get_task("transform_in_snowflake").downstream_task_ids == set()


def test_jsonplaceholder_dag_is_manual_only():
    dag = get_dag(JSONPLACEHOLDER_DAG_ID)

    assert getattr(dag, "schedule_interval", None) is None
    assert "Null" in dag.timetable.__class__.__name__
