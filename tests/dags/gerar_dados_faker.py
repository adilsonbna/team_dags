from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from faker import Faker


def gerar_dados_fake():
    # Criar listas vazias para armazenar os dados gerados para cada coluna do Dataframe
    # Obs: Ao executar dentro da função irá remover os dados já gerados.
    # Caso seja necessário manter os dados (append), criar as listas fora da função e comentar esta linha.
    n = 100
    fake = Faker("pt_BR")
    nome, sobrenome, cpf, rg, data_nascimento, celular, email, endereco, cidade, estado_nome, estado_sigla, cep, profissao, salario, data_admissao, data_demissao, func_ativo = [[] for i in range(0,17)]
    for row in range(0,n):
        nome.append(fake.first_name())
        sobrenome.append(fake.last_name())
        cpf.append(fake.cpf())
        rg.append(fake.rg())
        data_nascimento.append(fake.date_of_birth(minimum_age=16, maximum_age=100))
        celular.append(fake.cellphone_number())
        email.append(fake.ascii_email())
        endereco.append(fake.street_address())
        cidade.append(fake.city())
        estado_nome.append(fake.estado_nome())
        estado_sigla.append(fake.estado_sigla())
        cep.append(fake.postcode())
        profissao.append(fake.job())
        salario.append(fake.pydecimal(right_digits=2, positive=True, min_value=1000, max_value=50000))
        data_adm = fake.date_between_dates(date_start = "-60y", date_end= "-1y") # Salvar em uma variavel para garantir que a data de demissao seja após data admissao
        data_admissao.append(data_adm)
        
        ativo = fake.boolean()
        func_ativo.append(ativo)
        if ativo:
            data_demissao.append(None)
        else:
            data_demissao.append(fake.date_between_dates(date_start = data_adm, date_end= "now"))
        
        
    d = {"Nome": nome, "Sobrenome": sobrenome, "CPF": cpf, "RG": rg, "Data_Nascimento": data_nascimento, "Celular": celular,
        "Email": email, "Endereco": endereco, "Cidade": cidade, "Estado_Nome": estado_nome, "UF": estado_sigla, "CEP": cep,
        "Profissao": profissao, "Salario": salario, "Data_Admissao": data_admissao, "Data_Demissao": data_demissao, "Contrato_Ativo": func_ativo}

    df = pd.DataFrame(d)
    
    return(df)



dag = DAG('gera_dados_fake', description='Gerar Dados Faker',
          schedule_interval=timedelta(minutes=60),
          start_date=datetime(2017, 3, 20), catchup=False)

dados_faker = PythonOperator(task_id='gera_dados', python_callable=gerar_dados_fake, dag=dag)

dados_faker

