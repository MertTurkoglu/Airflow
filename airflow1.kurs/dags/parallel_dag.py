#paralellis,dag_concurrency,max_dag_runs_per_dag ,executor,subdag,taskgroup konularını anlama dagi
from airflow import DAG 
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_parallel_dag import subdag_parallel_dag
from airflow.utils.task_group import TaskGroup


default_args={

    'start_date':datetime(2020,1,1)
}

with DAG(dag_id='parallel_dag',schedule_interval='@daily',default_args=default_args,catchup=False) as dag :

    task_1=BashOperator(

        task_id='task_1',
        bash_command='sleep 3'
    )

    with TaskGroup('processing_tasks') as processing_tasks: 

        #taskleri gruplandırırken subdagi kullanma Taskgroup kullan 
        # processing_task group id oluyo as processing_tasks da task_group objesi oluyo
        #subdag de aşağıda yapılışı var daha complex bu yüzden TaskGroup ile yapıcaz
        task_2=BashOperator(

            task_id='task_2',
            bash_command='sleep 3'

        )
       # iç içe taskleri gruplayebilirsin yani bir task groubun içinde başka bir task grubu olabilir örneğin
       #spark ile çalışan taskler flink ile çalışan taskler
        with TaskGroup('spark_tasks') as spark_tasks:
            #aşağıdaki flink_tasks adlı groupla task idlerim aynı task_3 ikisindede
            #task id ler uniqe olmalıydı hata almayız burda çünkü task id yi şöyle okuyo
            # spark_tasks.task_3 ve flink_tasks.task_3
            task_3=BashOperator(

    
                task_id='task_3',
                bash_command='sleep 3'
           
        )

        with TaskGroup('flink_tasks') as flink_tasks:

            task_3=BashOperator(


    
                task_id='task_3',
                bash_command='sleep 3'
           
        )


    """processing=SubDagOperator( # processing adında sub dag oluşturcaz id si processing_task olucak

        task_id='processing_task',
        subdag=subdag_parallel_dag('parallel_dag','processing_task',default_args)
        # subdagi return eden fonksiyonun adı ama öncesinde dags klasörünün altında subdags klasörü                       
        #oluşturmak gerek sonrasonda subdags klasörünün altına
        # subdag_parallel_dag.py adında dosya oluşturuyoruz şimdi subdagsı return eden fonksiyonu buraya yazabiliriz
    )"""
   
    task_4=BashOperator(

        task_id='task_4',
        bash_command='sleep 3'
    )

    task_1>>processing_tasks >> task_4 
    # [task1 , task2,taskn] dizi içerisinde belirttiğim task ler paralel çalışabilir ama executorum                                 
    #sequential executor olduğu için paralel çalışmıcak sırayla çalışcak o yüzden executorumu local
    # executore çevirdim