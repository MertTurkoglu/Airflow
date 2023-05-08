from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
default_args={

    "start_date":datetime(2020,1,1)
}
with DAG(dag_id="deneme",default_args=default_args,schedule_interval='@daily',catchup=False) as dag :


    t1=dummy_task = DummyOperator(
        task_id="dummy_task",
       
    )

    