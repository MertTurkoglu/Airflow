from airflow.hooks.base import BaseHook # hook oluşturmak için Basehook u import etmek gerekir
from elasticsearch import Elasticsearch
from sqlalchemy.sql.schema import Index


class ElasticHook (BaseHook): # basehooku inheritance ediyom miras alıyom paramatrrelerini fonskiyonlarını
    def __init__(self, conn_id='elasticsearch_default',*args,**kwargs):#conn_id varsayılan olarak elasticsearch_default belirledik
        super().__init__(*args,**kwargs) #BaseHook classını başlattık method ve attributelerini kullanabiliriiz
        conn=self.get_connection(conn_id) # get_connection BaseHook sınıfından gelen method geriye Connection objesi döner
        #Connection sınıfının objesi olduğu için conn.Connectionsınıfındakiattributeler diyerek bu sınıftaki attributelere ulaşabilirim
        #örnek conn.host conn.port
        conn_config={} # bağlantı için gerekli configurasyonlar
        hosts=[] # elasticsearche bağlanacak makinelerin hostu biz tek makineden bağlancaz

        if conn.host:
            hosts=conn.host.split(',') # eğer birden çok host varsa onları ayırmak için
        
        if conn.port:
            conn_config['port']=int(conn.port) # port numarasını float olamsın int olsun istiyoruz

        if conn.login:
            conn_config['http.auth']=(conn.login,conn.password)
        
        self.es=Elasticsearch(hosts,**conn_config) # es Elasticsearch objesi
        self.index=conn.schema
    

    def info(self):
       return self.es.info() # elasticsearch objesi hakkında bilgi verir
    
    def set_index(self,index):
        self.index=index
    
    def add_doc(self,index,doc_type,doc): 
        # belli bir indexe data atmak için doc dediğimde döküman  elasticsearch objesine data atmak için
        self.set_index(index) # index
        result=self.es.index(index=index,doc_type=doc_type,body=doc)    
        return result



#plugins
#airflowu istediğimiz gibi özellişterebiliriz kendi operatörünü,hook views herşeyi özellişterebilirsin
# hook extarnal tool ile etkileşim kurmamımızı sağlar bu örnekte elasticsearch ile etkileşim kurduk
#bazen mevcut operatörler kendi işimizi halletmek için yeterli olmuyo kendi operatörümüzü yazcaz
#örneğin postgres den elasticsearch e data transfer etmek için bir operatör oluşturcaz 
#views arayüzde istediğimiz gibi değişiklik yapmayı sağlar örneğin yeni sayfa yeni sekme ekleme
#airflow 2.0 da sadece viewse izin var pluginslerde ama regular python modules ile diğerlerini dahil edebiliriz
#plugins leri oluşturduğumuz plugins klasörünün altında oluşturuyoruz



            






