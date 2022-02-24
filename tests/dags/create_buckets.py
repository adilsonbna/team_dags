from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from faker import Faker
from minio import Minio
from minio.error import S3Error
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook

#conn = BaseHook.get_connection('minio')
#print(conn)

def cria_buckets():
    # Criar listas vazias para armazenar os dados gerados para cada coluna do Dataframe
    # Obs: Ao executar dentro da função irá remover os dados já gerados.
    # Caso seja necessário manter os dados (append), criar as listas fora da função e comentar esta linha.



    conn = BaseHook.get_connection('minio')
    MINIO = str(conn.host) + ":" + str(conn.port)
    ACCESS_KEY = str(conn.login)
    SECRET_ACCESS = str(conn.password)
    client = Minio(MINIO, ACCESS_KEY, SECRET_ACCESS, secure=False)
    bucket = "a3team"

# Criar bucket Cadastro e RH
def createBucket():
    cadastro_faker = cadastro_faker
    rh_faker = rh_faker
    bucketC = str(bucket)+"/"+cadastro_faker
    bucketRH = str(bucket)+"/"+rh_faker 
    createBucketC = client.bucket_exists(bucketC)
    createBucketRH = client.bucket_exists(bucketRH)
    if not createBucketC:
       client.make_bucket(bucketC)
    if not bucketRH:
       client.make_bucket(createBucketRH)
    return (createBucketC, createBucketRH)




dag = DAG('create_buckets', description='Cria cadastro_faker e rh_faker',
          schedule_interval=timedelta(minutes=60),
          start_date=datetime(2017, 3, 20), catchup=False)

createBuckets = PythonOperator(task_id='create_buckets', python_callable=cria_buckets, dag=dag)

createBuckets

