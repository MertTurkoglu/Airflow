from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

with DAG(dag_id='latest_only_example', schedule_interval=timedelta(hours=6), start_date=datetime(2022, 1, 10), catchup=True) as dag:

    t1 = LatestOnlyOperator(task_id = 'latest_only')

    t2 = DummyOperator(task_id='task2')

    t3 = DummyOperator(task_id='task3')

    t4 = DummyOperator(task_id='task4')

    t5 = DummyOperator(task_id='task5')

    t1 >> [t2, t4, t5]
    
    
    # latest only operatörünü kullanan taske burada latest_only kullanıyo bu taske bağlı olan tasklerin çalışması için
    #bu taskin çalıştığı zaman scheduled_time ile next scheduled_time arasında ollmalı aksi halde bu taske bağlı olan taskler
    #çalışmaz scheduled time dediğim dag run un oluştuğu tarih işte bu dagi 7 ocak saat 15.55 çe çalıştırdım
    #scheduled__2022-01-07T06:00:00+00:00	
	#sscheduled__2022-01-07T00:00:00+00:00	
	#scheduled__2022-01-06T18:00:00+00:00	
	#scheduled__2022-01-06T12:00:00+00:00	
	#scheduled__2022-01-06T06:00:00+00:00	
	#scheduled__2022-01-06T00:00:00+00:00
    # task1 e bağlı olan tasklerin çalışması için çalıştığı zaman scheduled time ile next scheduled time arasında olamlı aksi halde çalışmaz
    # task 3 her türlü çalışcak çünkü LatestOnly operatörüne bağlı değil
 
