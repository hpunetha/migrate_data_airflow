from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.task_functions import send_success_email, send_failure_email
from etl.migrate import run_migration_in_batches

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


with DAG(
    'migrate_large_table_in_batches_dag',
    default_args=default_args,
    description='An advanced DAG with data extraction, transformation, and loading',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    etl = PythonOperator(
        task_id='migrate_large_table_in_batches_dag',
        python_callable=run_migration_in_batches,
        provide_context=True,
    )

    success_email = PythonOperator(
        task_id='send_success_email',
        python_callable=send_success_email,
        trigger_rule='all_success',
    )

    failure_email = PythonOperator(
        task_id='send_failure_email',
        python_callable=send_failure_email,
        trigger_rule='one_failed',
    )

    etl >> [success_email, failure_email]
