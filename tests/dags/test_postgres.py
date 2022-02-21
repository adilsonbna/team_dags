# [START import_module]
import pandas as pd
from minio import Minio
from os import getenv
from io import BytesIO
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook
# [END import_module]


# [START env_variables]
MINIO = getenv("MINIO", "node01.mycirrusit.com:31928")
ACCESS_KEY = getenv("ACCESS_KEY", "YOURACCESSKEY")
SECRET_ACCESS = getenv("SECRET_ACCESS", "YOURSECRETKEY")
POSTGRESQL = getenv("POSTGRESQL", "postgresql://postgres:postgres@k8s.mycirrusit.com:32094/airflow")
# [END env_variables]


# [START Postgres Connector]
conn = BaseHook.get_connection('postgres')
print(conn)
# print(f"AIRFLOW_CONN_{conn.conn_id.upper()}='{conn.get_uri()}'")
# [END Postgres Connector]

# [START default_args]
default_args = {
    'owner': 'david fachini',
    'start_date': datetime(2022, 2, 21),
    'depends_on_past': False,
    'email': ['david.fachini@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    'cadastro_faker',
    default_args=default_args,
    schedule_interval='@daily',
    tags=['development', 'faker', 'postgres'])
# [END instantiate_dag]

# [START basic_task]
# create table on postgres [if not exists] = postgresql [postgres]
create_faker_cadastro = PostgresOperator(
    task_id='create_faker_cadastro',
    postgres_conn_id='postgres_sql',
    sql='''CREATE TABLE fakerCadastro(
        custom_id integer NOT NULL, timestamp TIMESTAMP NOT NULL, user_id VARCHAR (50) NOT NULL
        );''',
    dag=dag)
# [END instantiate_dag]

# insert row in postgresql
insert_faker_row = PostgresOperator(
    task_id='insert_faker_row',
    postgres_conn_id='postgres_sql',
    sql='''INSERT INTO fakerCadastro VALUES(%s, %s, %s);''',
    # trigger_rule=TriggerRule.ALL_DONE,
    trigger_rule='all_done'
    # parameters=(uuid.uuid4().int % 123456789, datetime.now(), uuid.uuid4().hex[:10])
    dag=dag)

# [END basic_task]

# [START task_sequence]
create_faker_cadastro > insert_faker_row
# [END task_sequence]
