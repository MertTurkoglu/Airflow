# bu dag trigger kurallarını anlamak için
# xcom_dag3 de deki durum belli accurate çalışıyor inaccurate skip ediliyordu ve storing de skip ediliyordu
# bu durumu ortadan kaldırmak istiyoruz
# toplam 9 tane trigger kuralları var 

# 1-) all_success 
# [task_a,task_b]>> task_c şeklinde bir data pipelinın olsun
# task_a ve task_b succes ise task_C de success olcak demek ama eğer task_a fail değil taskb sucess ise 
# task_c upstream_failed statusune düscek ui da tasklerin statusune bakabilirsin işte skipped,success,running .......

#2-) all_failed 
# aynı şekilde data pipelenın var eğer task_a ve task_b failed ise task_c sucess olur
# eğer task_a success task_b fail ise task_c skipped olur

# 3-) all_done 
#  task a ve task b failed ise task c sucess olur
# task a succes task b fail ise task c de success olur

# 4-) one_success 
# eğer task_a success  task_c de sucess olur
# task c nin success olamsı için bir task in succes olması lazım

#5-) one_failed
# eğer task_a failed ise task_c sucess olur
#task cnin sucess olamsı için 1 taskin failed olamsı lazım

# 6-) none_failed 
# eğer task a ve task b skipped ise task c sucess olur eğer task a ve task b sucess ise task c de sucess olur

#7-) none_failed_or_skipped
# task a success task b skipped ise task c success olur xcom3 deki hatayı bunla çözcez
# none_failed den farkı bir tane success olmalı


from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

default_args={

    'start_date':datetime( 2020,1,1)
}

with DAG(dag_id='trigger_rule',schedule_interval='@daily',default_args=default_args,catchup=False) as dag :


    task_1=bash_task = BashOperator(
        task_id="task_1",
        bash_command='exit 1',
        do_xcom_push=False 
        #exit 1 demek bu task çalıştığında fail olasmı demek exit 0 demek task çalıştığında sucess olması demek
        
    )
    task_2=bash_task = BashOperator(
        task_id="task_2",
        bash_command='exit 1',
        do_xcom_push=False 
        
    )
    task_3=bash_task = BashOperator(
        task_id="task_3",
        bash_command='exit 0',
        do_xcom_push=False,
        trigger_rule='all_failed'
        
        
    )

    [task_1,task_2] >> task_3

# 1 ve 2 success olduğunda 3 de success oldu varsayılan olarak airflow böyle davranıyor peki bu davranışı değiştirelim
#yani trigger rule unu değiştirelim
# task 3 de trigger rule u all_failed yaptığımızda ve 1 ve 2 fail olduğunda task 3 sucess oldu
# trigger rule all_done ve  1 success 2 fail olduğunda 3 succes oldu
# trigger rule one_failed 1 fail 2 de sleep 30 diyince 3 success oldu one_failed de taskın 1 failed olduğunda diğer task 2 nin
#durumuna bakmadan task 3 success oldu
#xcom 3 de ki dagde storing taskinde trigger rule unu none_failed_or_skipped yapcaz
# 1 success 2 skipped olduğunda 3 success oluyordu bu rule tüm taskler failed olmucak ama en az bir task sucess olursa task 3 sucess olur demek