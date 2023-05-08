# hook bir extarnool bir tool a bağlantıyı içeren bir obje
# hook ile extarnal tool,db ,platform ile etkileşim sağlarım 
# hive ,s3 mysql,postgres,Hdfs ,pig ,gibi
# tablolar arası data trasnferi sağlar
# bir postgres tablosundaki dataları başksa bir postgres tablosuna kopyalıcaz

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

default_args = {
    'owner': 'Airflow',
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'start_date': datetime(2022, 1, 5),
}

dag = DAG('hooks_demo', default_args=default_args, schedule_interval='@daily',catchup=False)

def transfer_function(ds, **kwargs):

    query = "SELECT * FROM source_city_table"

    
    source_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')  # schema da hangi db ile etkileşim kuracağım
    source_conn = source_hook.get_conn()

    
    destination_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    destination_conn = destination_hook.get_conn()

    source_cursor = source_conn.cursor()  # source_cursor değişkeni  veritabanı üzerinde işlem yapmak için kullanacağımız imleç olacak. 
    destination_cursor = destination_conn.cursor() # target_city_table tablosuna veri eklemek için destination_cursor

    source_cursor.execute(query) # queryi yi çalıştırır
    records = source_cursor.fetchall() # çalıştırıdğımız query nin sonucu redords değişkeninde
    print(records)

    # eğer kayıt varsa
    if records: 
        execute_values(destination_cursor, "INSERT INTO target_city_table VALUES %s", records) 
        # rowları tek tek değilde hepsini tek seferde eklemek için 
        destination_conn.commit() # Sorgunun veritabanı üzerinde geçerli olması için commit işlemi gerekli.

    source_cursor.close()
    destination_cursor.close()
    source_conn.close()
    destination_conn.close()
    print("Data transferred successfully!")


t1 = PythonOperator(task_id='transfer', python_callable=transfer_function, provide_context=True, dag=dag)
# airflow clı a girmek için dokcer exec -it airflowcontaineridcontainerid bash 
# posgtgres clı ına girmek için docker exec -it postgresconatiner_id bash
# psql -U airflow
#\dt default table ları gösterir
# ki postgres benim metadatabase im burada metadatabase de tutulan verileri görebilirim
# şimdi aşağıdaki gibi sql leri yazıyorum
        
    #create table target_city_table as (select * from source_city_table);
# insert into source_city_table (city_name, city_code) values('New York', 'ny'), ('Los Angeles', 'la'), ('Chicago', 'cg'), ('Houston', 'ht');
# select * from target_city_table;

# ui dan connectionu oluşturman lazım host postgres schema login password airflow
