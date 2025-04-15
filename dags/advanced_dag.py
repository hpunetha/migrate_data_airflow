from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.task_functions import extract_data, transform_data, load_data, send_success_email, send_failure_email

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'advanced_dag',
    default_args=default_args,
    description='An advanced DAG with data extraction, transformation, and loading',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
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

    extract >> transform >> load >> [success_email, failure_email]
