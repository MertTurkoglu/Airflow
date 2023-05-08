from airflow import DAG
from datetime import datetime,timedelta
from airflow.sensors.hdfs_sensor import HdfsSensor
from airflow.operators.bash_operator import BashOperator

default_args = {

    'start_date':datetime(2020,5,20)
    

}
yesterday_year = datetime.strftime(datetime.now() - timedelta(1), '<%Y>')
yesterday_month=datetime.strftime(datetime.now() - timedelta(1), '<%m>')
yesterday_day=datetime.strftime(datetime.now() - timedelta(1), '<%d>')
folder='/home/train/hadoop/'+yesterday_year+'/'+yesterday_month+'/'+yesterday_day

with DAG(dag_id='quiz1',default_args=default_args,schedule_interval='@daily',catchup=False) as dag:

    
    check_folder=HdfsSensor(

        task_id='check_folder',
        hdfs_conn_id='hdfs_default',
        filepath='/home/train/hadoop/'+yesterday_year+'/'+yesterday_month+'/'+yesterday_day
    )

    create_file = BashOperator(
        task_id="crate_file",
        bash_command='hdfs dfs -touch'+folder+'/_seen'
    )

    check_folder>>create_file
       

