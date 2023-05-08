from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.oracle_operator import OracleOperator
from airflow.operators.sensors import SqlSensor

default_args={

    'start_date':datetime(2022,1,4),
    

}

with DAG( dag_id='quiz2_1',default_args=default_args,schedule_interval='@monthly',catchup=False) as dag :


    t1 = SqlSensor(
        task_id='check_data_arrived',
        conn_id="oracle_conn",
        sql="SELECT COUNT(*) ceyhandb.pipeline;",
        poke_interval=600,
        timeout=1800
)


sqoop import # bu komutu hangi operatörde kullanacğımı bilemedim
--connect "jdbc:oracle:........" 
--username "root "
--password "cloudera" 
--hive-import 
--query "select * from ceyhandb.pipeline where pdate=SELECT TO_DATE(current_date - 1) AS yesterday_date From Dual"
--hive-table "......" 
--hive-partition-key "pdate "
--target-dir /user/sqoop_import/"....."
--split-by id 