from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mmds_engineer',
    'start_date': datetime(2026, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='orders_realtime_pipeline',

    default_args=default_args,

    schedule_interval='*/10 * * * *',

    catchup=False,

    max_active_runs=1,

    description='Orders API -> Spark -> ClickHouse Analytics Pipeline'

) as dag:

    # =========================================
    # FETCH DATA FROM API
    # =========================================

    fetch_orders = BashOperator(
        task_id='fetch_orders_data',

        bash_command=(
            'python '
            '/opt/airflow/dags/scripts/fetch.py'
        )
    )

    # =========================================
    # PROCESS ANALYTICS USING SPARK
    # =========================================

    process_orders = BashOperator(
        task_id='process_orders_analytics',

        bash_command=(
            'python '
            '/opt/airflow/dags/scripts/process.py'
        )
    )

    # =========================================
    # TASK ORCHESTRATION
    # =========================================

    fetch_orders >> process_orders