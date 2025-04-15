from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.task_functions import send_success_email, send_failure_email
from etl.migrate import run_migration_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


with DAG(
    'migrate_s3_data_dag',
    default_args=default_args,
    description='Dag for reading data from s3 bucket or minio and copy to mariadb',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    etl = PythonOperator(
        task_id='migrate_s3_data_dag',
        python_callable=run_migration_s3,
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
