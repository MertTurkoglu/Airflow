from _typeshed import WriteableBuffer
from airflow import DAG
from datetime import datetime,timedelta
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.email_operator import EmailOperator

default_args = {

    'start_date':datetime(2020,5,20),
    "email": ['Linux_admin@dilisim.com',],
    "email_on_failure": False,
    "email_on_retry": False,

}


with DAG(dag_id='quiz2_3',default_args=default_args,schedule_interval='@daily',catchup=False) as dag:

    transfer_data_local_to_local = SFTPOperator(
    task_id="local_to_local",
    ssh_conn_id="ssh_default",
    local_filepath="/a/b/final",
    remote_filepath="/a/b/final",
    operation="put",
    create_intermediate_dirs=True
    )

    check_file_exists = FileSensor(
        task_id='check_file_exists',
        filepath='/a/b/final',
        fs_conn_id='fs_default',
        poke_interval=5,
        timeout=25,

        soft_fail=True
    )

    send_email = EmailOperator(
        task_id='send_email',
        to='Linux_admin@dilisim.com',
        subject='Dosya mevcut degil',
        html_content=""" <h1> DOSYA MEVCUT DEGIL </h1> """,
        trigger_rule='one_failed'
    )
    transfer_data_local_to_local>>check_file_exists>>send_email