from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    #bu paramatre kendinden önceki dag run daki task in durumu ile ilgili True ise diyelimki dünkü dag run daki task x fail oldu bugünkü dag run da ki task x çalışmaz
    # çalışması için dünkü dag run daki taskin success olması lazım
    # false ise bu paramtre o zman kendinden 
    # önceki dag runlar daki taskın  durumuna bakmadan çalışır
    "start_date": datetime(2022, 1, 2),
    #airflowda task fail veya çalışması için tekrar denendiği durumlarda istediğim adrese mail atabilirim
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,  #task fail olduğunda kaç defa tekrar denencek
    "retry_delay": timedelta(minutes=5), # kaç dakka aralıklarla denencek
   
}
# defaults args da belirttiğim her paramatre tüm tasklerde geçerli ama taska özel değiştirebilirim
#t2 de mesela retries ı 3 yaptım t2 taskı için retries 3 olcak ama t1 ve t3 taskı için retries ı bir olcak

dag = DAG("tutorial", default_args=default_args, schedule_interval="@daily",catchup=False)


t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}" 
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""
# ilk echo şuanki date i görüntüler
# ikincisi date e 7 gün ekelyip gösterip
# 3.sü Parameter I passed in" ifadesini görüntüler

t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

t2.set_upstream(t1) # task 1 succes olduktan sonra t2 taski çalışcak demek
t3.set_upstream(t1) # task1 success olduktan sonra task 3 çalışcak demek

# taskler arası bağlılığı >> ,<< , set_upstream , set_downstream methodlarıyla sağlayabilirim
