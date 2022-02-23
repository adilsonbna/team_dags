
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'adilson',
    'depends_on_past': False,
    'email': ['adilsonbna@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    'twitter_get_data',
    default_args=default_args,
    description='Get Tweets',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['twitter', 'getdata'],
) as dag:
    t1 = BashOperator(
        task_id='get_tweets',
        bash_command='/usr/bin/python3.6 /cluster/helm/airflow/Python_twitter.py',
    )

    t1