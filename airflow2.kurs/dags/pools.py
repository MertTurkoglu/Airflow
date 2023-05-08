from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 5),
    }

with DAG("pools", default_args=default_args, schedule_interval='@daily',catchup=False) as dag:

    t1 = BashOperator(task_id="task-1", bash_command="sleep 5", pool="pool_1",catchup=False)

    t2 = BashOperator(task_id="task-2", bash_command="sleep 5", pool="pool_1")

    t3 = BashOperator(task_id="task-3", bash_command="sleep 5", pool="pool_2", priority_weight=2)

    t4 = BashOperator(task_id="task-4", bash_command="sleep 5", pool="pool_2")

# taskler arasında hiç bir bağlılık yok buda demek 4 task de paralel olarak çalışabilir
# ui dan pool oluşturabilirsin
# pool workerlara eşit sayıda task dağıtmak için pool kullanıcaz
# pool_1 de 3 worker var pool 2 de 1 worker var 
# t1 ve t2 pool1 de çalışcak t3 ve t4 pool 2 de çalışcak
#pool 1 de 3 worker olduğu için bu poolu kullanan tasklerden 3 tane taskı paralel çalıştırabilirim demek
# pool_2 de 1 worker olduğu için aynı anda tek 1 task çalıştırabilirim demek buda bu poolu kullanan taskler tek tek çalışcak demek
# # bu yüzden t1 ve t2 ve t4 paralel çalışcak  sonra t3 çalışcak
# eğer priority_weight paramatresini iki yapmasydım varsayılan olarak bu değer 1 kuyrukta bekleyen tasklerin çalıştırma önceliği ozman t4 önce çalışcaktı
# task 3 de 2 yaptık bu değeri t4 de varsayılan olarak 1 olduğu için büyük olan değer yani task3 önce çalışcak
