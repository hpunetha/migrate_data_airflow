from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.task_functions import task_1, task_2  # Import the functions from the script

# Define the DAG
with DAG(
    'example_dag_2',
    description='A DAG with dependent tasks',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:
    # Define task 1 using PythonOperator
    task_1_operator = PythonOperator(
        task_id='task_1',
        python_callable=task_1
    )

    # Define task 2 using PythonOperator
    task_2_operator = PythonOperator(
        task_id='task_2',
        python_callable=task_2
    )

    # Set task dependencies
    task_1_operator >> task_2_operator
    # This means that task_2_operator will run only after task_1_operator completes successfully
