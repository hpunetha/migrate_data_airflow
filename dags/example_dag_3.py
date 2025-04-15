from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.task_functions import task_1, task_2, task_3  # Import the functions from the script

# Define the DAG
with DAG(
    'example_dag_3',
    description='A DAG with a failure path',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False  #prevent the scheduler from running past executions.
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

    # Define task 3 using PythonOperator
    task_3_operator = PythonOperator(
        task_id='task_3',
        python_callable=task_3,
        trigger_rule='one_failed'  # Execute task_3 if any upstream task fails
    )

    # Set task dependencies
    task_1_operator >> task_2_operator
    # This means that task_2_operator will run only after task_1_operator completes successfully

    task_2_operator >> task_3_operator
    # This means that task_3_operator will run only if task_2_operator fails
