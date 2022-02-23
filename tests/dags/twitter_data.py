
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Adilson Cesar',
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
    tags=['twitter', 'getdata', 'dev'],
) as dag:
    #ssh_connection = SSHOperator(
    #   ssh_conn_id='ssh_node01',
    #   task_id='connected_to_node01',
    #    command='scp -rp root@node01.mycirrusit.com:/cluster/helm/airflow/Python_twitter.py /tmp',
    #    dag=dag
    #    )

    t2 = BashOperator(
        task_id='get_tweets',
        bash_command='/usr/bin/python3.7 /tmp/Python_twitter.py',
    )

    t2