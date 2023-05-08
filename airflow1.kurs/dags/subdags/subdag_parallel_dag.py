from airflow import DAG
from airflow.operators.bash import BashOperator

def subdag_parallel_dag(parent_dag_id,child_dag_id,default_args): 
    # bu fonksiyon bazı parametreler alıyor
    #parent_dag_id en üst deki dag in id si yani "parallel_dag"
    #child_dag_id sub dag olacak id yani "processing_task" tasklleri gruplamak istediğimiz task buda bir dag oluyo çünkü birden çok task var
    #parallel_dag daki default_args parametrem le subdag deki default_args paramatresi aynı olmalı yani default_args={     'start_date':datetime(2020,1,1) }

    with DAG(dag_id=f'{parent_dag_id}.{child_dag_id}',default_args=default_args) as dag:





        task_2=BashOperator(

            task_id='task_2',
            bash_command='sleep 3'

        )


        task_3=BashOperator(




            task_id='task_3',
            bash_command='sleep 3'
           
        )

        return dag
    





        







