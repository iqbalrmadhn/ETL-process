from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import requests, pandas as pd
import sqlalchemy

username= 'iqbal'
password= 'iqbal'
username1= 'root'
password1= 'root'
host= 'host.docker.internal'
db_mysql= 'staging_area'
db_postgre= 'data_warehouse'
port_mysql = 3307
port_postgre = 5433
setting_mysql = f'{username}:{password}@{host}:{port_mysql}/{db_mysql}'
setting_postgre = f'{username1}:{password1}@{host}:{port_postgre}/{db_postgre}'

def getdata (url_data,ti):
    r = requests.get(url_data)
    data = r.json()
    df = pd.DataFrame(data['data']['content'])
    df_xcom = df.to_json(orient = 'records')
    ti.xcom_push(key='raw_data',value=df_xcom)
    

def insert_into_table(ti,eng_mysql):
    data= ti.xcom_pull(key='raw_data')
    df=pd.read_json(data,orient = 'records')
    conn = sqlalchemy.create_engine(f'mysql+mysqlconnector://{eng_mysql}')
    df.to_sql(con=conn, name='staging_table', if_exists='replace', index=False)

def migrate(eng_mysql,eng_postgre):
    conn_mysql = sqlalchemy.create_engine(f'mysql+mysqlconnector://{eng_mysql}')
    conn_postgre = sqlalchemy.create_engine(f'postgresql+psycopg2://{eng_postgre}')
    df = pd.read_sql(sql='staging_table', con=conn_mysql)
    df.to_sql(con=conn_postgre, name='raw_data_table', if_exists='replace', index=False)

def temp_tabel_case(eng_postgre,ti):
    data= ti.xcom_pull(key='raw_data')
    df=pd.read_json(data,orient = 'records')
    column_before = df.columns
    status = []
    status_detail = []
    for x in column_before:
        if x.isupper():
            status.append(x)
        else:
            status_detail.append(x)
    column_after = []
    for i in status:
        for j in status_detail:
            split = j.split("_")
            if i.lower() in split:
                column_after.append([split[0].lower(),split[1]])
    df = pd.DataFrame(column_after, columns=['status','status_detail'])
    conn_postgre = sqlalchemy.create_engine(f'postgresql+psycopg2://{eng_postgre}')
    df.to_sql(con=conn_postgre, name='temp_tabel_case', if_exists='replace', index=False)

with DAG(
    dag_id='final_project_schedule',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False
) as dag:
    get_url = PythonOperator(
        task_id='get_url',
        python_callable=getdata,
        op_kwargs={"url_data": "https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian?level=kab"}
    )
    insert_to_staging_area = PythonOperator(
        task_id='insert_to_staging_area',
        python_callable=insert_into_table,
        op_kwargs={"eng_mysql": setting_mysql}
    )
    migrate_data = PythonOperator(
        task_id='migrate_data',
        python_callable=migrate,
        op_kwargs={"eng_mysql": setting_mysql, "eng_postgre": setting_postgre}
    )
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="con_data_warehouse",
        sql="sql_file/create_table.sql"
    )
    temp_table = PythonOperator(
        task_id='temp_table',
        python_callable=temp_tabel_case,
        op_kwargs={"eng_postgre": setting_postgre}
    )
    insert_table_dim = PostgresOperator(
        task_id="insert_table_dim",
        postgres_conn_id="con_data_warehouse",
        sql="sql_file/data_table_dim.sql"
    )
    insert_table_fact = PostgresOperator(
        task_id="insert_table_fact",
        postgres_conn_id="con_data_warehouse",
        sql="sql_file/data_table_fact.sql"
    )
get_url >> insert_to_staging_area >> migrate_data >> create_table >> temp_table >> insert_table_dim >> insert_table_fact