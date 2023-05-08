from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import DataTransferOperator,FileCountSensor


dag = DAG('plugins_dag2', schedule_interval=timedelta(1), start_date=datetime(2022, 1,8), catchup=False)

t1 = FileCountSensor(
    task_id = 'file_count_sensor',
    dir_path = '/usr/local/airflow/plugins',
    conn_id = 'fs_default',
    poke_interval = 5,
    timeout = 100,
    dag = dag
)
