# File: /opt/airflow/dags/dummy_test_dag.py

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
        dag_id="dummy_test_dag",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
) as dag:
    t1 = DummyOperator(task_id="start")

dag  # <- important!
# This is a dummy DAG that does nothing. It is used for testing purposes only.
