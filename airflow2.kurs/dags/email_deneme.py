from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 2),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}



with DAG(dag_id='email_deneme',default_args=default_args,schedule_interval='@daily',catchup=False) as dag:


    check_file_exists = FileSensor(
        task_id='check_file_exists',    
        filepath='/usr/local/airflow/store_files_airflow/raw_store_transactions.csv',  
        fs_conn_id='fs_default',
        poke_interval=5, 
        timeout=25,
        soft_fail=True
        

    )

    send_email = EmailOperator(
        task_id='send_email',
        to='mertturkoglu26@airflow.com',
        subject='Daily report generated',
        html_content=""" <h1> DOSYA MEVCUT DEGIL </h1> """,
        trigger_rule='one_failed'
    )

    check_file_exists>>send_email

