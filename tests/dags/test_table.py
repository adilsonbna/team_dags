# [START import_module]
from airflow.models import DAG
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
# [END import_module]

# [START Postgres Connector]
conn = Connection(uri="postgresql://postgres:XAExoupGId@k8s.mycirrusit.com:32094/airflow")
# [END Postgres Connector]

# [START default_args]
default_args = {
    'owner': 'david fachini',
    'depends_on_past': False,
    'email': ['david.fachini@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)}
# [END default_args]


# [START instantiate_dag]
dag_params = {
    'dag_id': 'PostgresOperator_dag',
    'start_date': datetime(2022, 02, 21),
    'schedule_interval': None
}

with DAG(**dag_params) as dag:

    create_table = PostgresOperator(
        task_id='create_fakerCadastro',
        sql='''CREATE TABLE fakerCadastro(
            custom_id integer NOT NULL, timestamp TIMESTAMP NOT NULL, user_id VARCHAR (50) NOT NULL
            );''',
    )
# [END instantiate_dag]

# [START basic_task]
insert_row = PostgresOperator(
    task_id='insert_faker_row',
    sql='INSERT INTO create_fakerCadastro VALUES(%s, %s, %s)',
    trigger_rule=TriggerRule.ALL_DONE,
    parameters=(uuid.uuid4().int % 123456789, datetime.now(), uuid.uuid4().hex[:10])
)
# [END basic_task]

# [START task_sequence]
create_fakerCadastro > insert_faker_row
# [END task_sequence]
