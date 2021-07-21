# Primeira DAG com Airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Argumentos default
default_args = {
    'owner': 'Bruno Coutinho',
    'depends_on_part': False,
    'start_date': datetime(2021, 7, 20, 10, 40),
    'email': 'bruno@airflow.com.br',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Definir a DAG - Fluxo
dag = DAG(
    "Treino-01",
    description="Básico de Bash Operators e Python Operators",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

# Adicionar tarefas
hello_bash = BashOperator(
    task_id='Hello_Bash',
    bash_command='echo "Hello Airflow from Bash"',
    dag=dag
)


def say_hello():
    print("Hello Airflow from Python")


hello_python = PythonOperator(
    task_id='Hello_Python',
    python_callable=say_hello,
    dag=dag
)

hello_bash >> hello_python
