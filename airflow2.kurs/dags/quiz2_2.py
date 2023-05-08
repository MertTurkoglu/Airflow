from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.oracle_operator import OracleOperator
from airflow.operators.sensors import SqlSensor

default_args={

    'start_date':datetime(2020,5,20),
    

}

with DAG( dag_id='quiz2_1',default_args=default_args,schedule_interval='@daily',catchup=False) as dag :


    t1 = SqlSensor(
        task_id='check_data_arrived',
        conn_id="oracle_conn",
        sql="SELECT COUNT(*) main.a1;",
        poke_interval=600,
        timeout=1800
)

sqoop export --connect \
jdbc:jdbc:oracle:................ \
 --table elvan1.a1 \
 --username root --password cloudera \
 --export-dir /user/hive/warehouse/main.db/a1\
 --input-fields-terminated-by ','