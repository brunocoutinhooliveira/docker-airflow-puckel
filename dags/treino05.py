# Integrações para entrega
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
import zipfile

# Constantes
data_path = '/usr/local/airflow/data/3.DADOS/'
arquivo = data_path + 'microdados_enade_2019.txt'

default_args = {
    'owner': 'Bruno Coutinho',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 21, 0, 45),
    'email': ["bruno@airflow.com.br"],
    'email_on_failure': False,
    'email_on_retry': False
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    "Treino-05",
    description="Uma dag com vários paralelismos",
    default_args=default_args,
    schedule_interval=None
)

start_preprocessing = BashOperator(
    task_id='start_preprocessing',
    bash_command='echo "Start Preprocessing! Vai!"',
    dag=dag
)

get_data = BashOperator(
    task_id='get-data',
    bash_command='curl https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /usr/local/airflow/data/microdados_enade_2019.zip',
    trigger_rule='all_done',
    dag=dag
)


def unzip_file():
    with zipfile.ZipFile('/usr/local/airflow/data/microdados_enade_2019.zip', 'r') as zipped:
        zipped.extractall('/usr/local/airflow/data')


unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_file,
    dag=dag
)


def aplica_filtros():
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE',
            'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
    enade = pd.read_csv(arquivo, sep=';', decimal=',', usecols=cols,
                        encoding="latin1",
                        dtype={'NU_IDADE': 'float64'},
                        na_values={"NA", " "})
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]
    enade.to_csv(data_path + 'enade_filtrado.csv', index=False)


task_aplica_filtro = PythonOperator(
    task_id='aplica_filtro',
    python_callable=aplica_filtros,
    dag=dag
)

# Idade centralizada na média
# Idade centralizada ao quadrado


def constroi_idade_centralizada():
    idade = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['NU_IDADE'])
    idade['idadecent'] = idade.NU_IDADE.mean()
    idade[['idadecent']].to_csv(data_path + "idadecent.csv", index=False)


def constroi_idade_cent_quad():
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadecent['idade2'] = idadecent.idadecent ** 2
    idadecent[['idade2']].to_csv(data_path + 'idadequadrado.csv', index=False)


task_idade_cent = PythonOperator(
    task_id='constroi_idade_centralizada',
    python_callable=constroi_idade_centralizada,
    dag=dag
)

task_idade_quad = PythonOperator(
    task_id='constroi_idade_ao_quadrado',
    python_callable=constroi_idade_cent_quad,
    dag=dag
)


def constroi_est_civil():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I01'])
    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outro'
    })
    filtro[['estcivil']].to_csv(data_path + 'estcivil.csv', index=False)


task_est_civil = PythonOperator(
    task_id='constroi_est_civil',
    python_callable=constroi_est_civil,
    dag=dag
)


def constroi_cor():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I02'])
    filtro['cor'] = filtro.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indígena',
        'F': "",
        ' ': ""
    })
    filtro[['cor']].to_csv(data_path + 'cor.csv', index=False)


task_cor = PythonOperator(
    task_id='controi_cor_da_pele',
    python_callable=constroi_cor,
    dag=dag
)


def constroi_escopai():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I04'])
    filtro['escopai'] = filtro.QE_I04.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5
    })
    filtro[['escopai']].to_csv(data_path + 'escopai.csv', index=False)


task_escopai = PythonOperator(
    task_id='controi_escopai',
    python_callable=constroi_escopai,
    dag=dag
)


def constroi_escomae():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I05'])
    filtro['escomae'] = filtro.QE_I05.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5
    })
    filtro[['escomae']].to_csv(data_path + 'escomae.csv', index=False)


task_escomae = PythonOperator(
    task_id='controi_escomae',
    python_callable=constroi_escomae,
    dag=dag
)


def constroi_renda():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I08'])
    filtro['renda'] = filtro.QE_I08.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5,
        'G': 6
    })
    filtro[['renda']].to_csv(data_path + 'renda.csv', index=False)


task_renda = PythonOperator(
    task_id='controi_renda',
    python_callable=constroi_renda,
    dag=dag
)

# Task de JOIN


def join_data():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv')
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadeaoquadrado = pd.read_csv(data_path + 'idadequadrado.csv')
    estcivil = pd.read_csv(data_path + 'estcivil.csv')
    cor = pd.read_csv(data_path + 'cor.csv')
    escopai = pd.read_csv(data_path + 'escopai.csv')
    escomae = pd.read_csv(data_path + 'escomae.csv')
    renda = pd.read_csv(data_path + 'renda.csv')

    final = pd.concat([
        filtro, idadecent, idadeaoquadrado, estcivil, cor,
        escopai, escomae, renda
    ],
        axis=1
    )

    final.to_csv(data_path + 'enade_tratado.csv', index=False)
    print(final)


task_join = PythonOperator(
    task_id='join_data',
    python_callable=join_data,
    dag=dag
)

# Ingestão no SQL Server


def escreve_dw():
    final = pd.read_csv(data_path + 'enade_tratado.csv')
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://higher:9SNq.SB:=ce7jtV0W#U?@localhost/enade")
    # engine = sqlalchemy.create_engine(
    #   "mssql+pyodbc://SA:Bruno19770121@127.0.0.1/enade?driver=ODBC+Driver+17+for+SQL+Server")

    final.to_sql("tratado", con=engine, index=False, if_exists='append')


task_escreve_dw = PythonOperator(
    task_id='escreve_dw',
    python_callable=escreve_dw,
    dag=dag
)

start_preprocessing >> get_data >> unzip_data >> task_aplica_filtro
task_aplica_filtro >> [task_idade_cent, task_est_civil, task_cor,
                       task_escopai, task_escomae, task_renda]

task_idade_quad.set_upstream(task_idade_cent)

task_join.set_upstream([
    task_idade_cent, task_est_civil, task_cor, task_idade_quad,
    task_escopai, task_escomae, task_renda
])

task_join >> task_escreve_dw
