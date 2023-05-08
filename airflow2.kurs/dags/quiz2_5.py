from airflow  import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "start_date": datetime(2022, 1, 8)
}

with DAG( dag_id='quiz2_6',default_args=default_args,schedule_interval='@daily',catchup=False) as dag :

    a5=DummyOperator(task_id='a5')

    a4=DummyOperator(task_id='a4')

    a3=DummyOperator(task_id='a3')

    a2=DummyOperator(task_id='a2')

    a1=DummyOperator(task_id='a1')


    #a5>>[a4,a3]
    #a4>>[a2,a1]
    a3.set_upstream(a5) 
    a4.set_upstream(a5)
    a2.set_upstream(a4)
    a1.set_upstream(a4)
    
   
