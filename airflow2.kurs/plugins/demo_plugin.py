from airflow.plugins_manager import AirflowPlugin # plugin oluşturmamız için AirflowPlugini import etmemeiz gerekir
from airflow.models import BaseOperator # kendi operatörümüzü oluşturmak için
from airflow.operators.sensors import BaseSensorOperator #kendi sensör operatörümüzü oluşturmak için
import logging as log
from airflow.utils.decorators import apply_defaults 
#her operatör için girdiğimiz paramatreler var task id owner start date catchup bu paramatreleri dahil etmek için
from airflow.contrib.hooks.fs_hook import FSHook #kendi sensör operatörüm için fileSysteme bağlanmalıyım bunun içim FSHokk u import ettik
import os, stat
# aşağıdakilerde kendi hook unu oluşturmak için gerekli import
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook


class DataTransferOperator(BaseOperator):
#kendi operatörümüzü yazcaksak her zaman şunları yapmalıyız  BaseOperator sınıfını inheritance etcez
# bu sınıftan constructorunu ve execute methodunnu override etcez
#zaten airflowdaki dğer operatörlerde bu classı inheritance edilerek yazılmış
#bu operatör text dosyasındaki kelimelerden istenilen kelimeleri silip bir konuma kaydetcek
#
    @apply_defaults
    def __init__(self, source_file_path, dest_file_path, delete_list, *args, **kwargs):
#bizim yazcağım operatördeki paramatreler dosyanın konumu ,nereye kopyalanacağı ve dosyadaki hangi kelimlerin kaldıralacağı
#bunları constructorumuzda oluşturduk ve BaseOperatordeki paramatreleri de tek tek yazmamak için Baseoperatörünün
#constructorunu çağırdık super().__init__(*args, **kwargs) ifadesiyle

        self.source_file_path = source_file_path
        self.dest_file_path = dest_file_path
        self.delete_list = delete_list
        super().__init__(*args, **kwargs)
#operatörün asıl yapacığı işi bu methodda yazıyoruz
    def execute(self, context):

        SourceFile = self.source_file_path
        DestinationFile = self.dest_file_path
        DeleteList = self.delete_list

        log.info("### custom operator execution starts ###")
        log.info('source_file_path: %s', SourceFile)
        log.info('dest_file_path: %s', DestinationFile)
        log.info('delete_list: %s', DeleteList)

        fin = open(SourceFile)
        fout = open(DestinationFile, "a") # append modda açıyoruz 

        for line in fin:
            log.info('### reading line: %s', line)
            for word in DeleteList:
                log.info('### matching string: %s', word)
                line = line.replace(word, "")

            log.info('### output line is: %s', line)
            fout.write(line)

        fin.close()
        fout.close()
class FileCountSensor(BaseSensorOperator):

# aynı şekilde bir sensor operatörü oluşturmak içinde BaseSensorOperator class ını import etcez ve
# methodlarını override etcez tek fark execute methodunun yerini poke methodu alıyo burda
# bu operatörde istediğimiz konumda 5 den fazla klasör varsa true dönmesini istiyoruz
#sensor operatorleri boolean return eden operatörlerdir

    @apply_defaults
    def __init__(self, dir_path, conn_id, *args, **kwargs):
# bu operatör için bir klasör konumuna ihtiyacım var bir de conn_id ye 
# sensor operatörleri için bir hook bağlantısı gerekli onuda oluşturcaz conn_id de onun için
        self.dir_path = dir_path
        self.conn_id = conn_id
        super().__init__(*args, **kwargs)

    def poke(self,context):
        hook = FSHook(self.conn_id) # fileSystem bağlantısı için #filesytem e bağlanacak conn_id default_fs varsayılan olarak
        #filesytem e bağlanacak conn_id
        basepath = hook.get_path() # root klasörüm oluyor
        self.log.info('basepath:{}'.format(basepath))
        full_path = os.path.join(basepath, self.dir_path)
        self.log.info('poking location %s', full_path)
        try:
            for root, dirs, files in os.walk(full_path):
                self.log.info((root,dirs,files))
                if len(files) >= 5:
                    return True
        except OSError:
            return False
        return False


class MySQLToPostgresHook(BaseHook):
# yine aynı şekil kendi hookunu yazmak için BaseHook u inheritance etcen
    def __init__(self):
        print("##custom hook started##")

    def copy_table(self, mysql_conn_id, postgres_conn_id):

        print("### fetching records from MySQL table ###")
        mysqlserver = MySqlHook(mysql_conn_id)
        sql_query = "SELECT * from mysql_city_table "
        data = mysqlserver.get_records(sql_query)

        print("### inserting records into Postgres table ###")
        postgresserver = PostgresHook(postgres_conn_id)
        postgres_query = "INSERT INTO postgres_city_table VALUES(%s, %s, %s);"
        postgresserver.insert_rows(table='postgres_city_table', rows=data)

        return True



class DemoPlugin(AirflowPlugin):
    name = "demo_plugin"
    operators = [DataTransferOperator]
    sensors = [FileCountSensor]
    hooks = [MySQLToPostgresHook]
