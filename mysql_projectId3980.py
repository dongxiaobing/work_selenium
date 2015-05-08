#!/bin/python
#coding=utf-8
import sys   
reload(sys)  
sys.setdefaultencoding('utf8')  
import MySQLdb
import json
import unittest
from time import sleep
import datetime
import time
 

productId_list=[]
class TestProject3980PublishDate(unittest.TestCase):
    def setUp(self):
        self.sql_limit_num=10
        self.host_search="10.255.254.22"
        self.host_product="10.255.255.22"
        self.db_search="prodviewdb"
        self.db_product="ProductDB"
        self.user="writeuser"
        self.passwd="ddbackend"
        self.publish_date_finally="1970-01-01 00:00:00"
        self.publish_date_0001="0001-01-01 00:00:000"
        self.publish_date_small="1909-12-31 23:59:59"
        self.publish_date_equal="1910-01-01 00:00:00"
        self.publish_date_big="1910-01-01 00:00:01"
        
        self.last_changed_date='2015-05-07 00:01:00'
        
        self.process_last_date='2015-05-07 00:00:00'
    def tearDown(self):
        pass
    def updateinfo_to_mysql(self,sql_cmd,host,user,passwd,db):
        print sql_cmd
        conn=MySQLdb.connect(host=host,user=user,passwd=passwd,db=db,charset="utf8")
        cursor = conn.cursor()
        num=5
        n = cursor.execute(sql_cmd)
        conn.commit()
        cursor.close()
        conn.close()
    def getinfo_from_mysql(self,sql_cmd,host,user,passwd,db):
        #print "sql cmd"
        #print sql_cmd
        product_list=[]
        conn=MySQLdb.connect(host=host,user=user,passwd=passwd,db=db,charset="utf8")
        cursor = conn.cursor()
        n = cursor.execute(sql_cmd)
        for row in cursor.fetchall():
            for i in row:    
                product_list.append(i)
                #print "!",i
        cursor.close()
        #cursor.commit()
        conn.close()
        return product_list
    def get_product_id_list_from_prodviewdb(self):
        """获取产品id"""
        sql_get_product_id="select product_id from products_core_search order by product_id desc limit 50;" 
        prodviewdb_product_id_list=self.getinfo_from_mysql(sql_get_product_id,self.host_search,self.user,self.passwd,self.db_search)
        #print "产品id为："
        return prodviewdb_product_id_list
    def get_product_id_list_from_ProductDB(self):
        """获取产品id"""
        sql_get_product_id="select product_id from Products_Core order by product_id desc limit 50;"
        product_product_id_list=self.getinfo_from_mysql(sql_get_product_id,self.host_product,self.user,self.passwd,self.db_product)
        #print "产品id为："
        return product_product_id_list
    def get_publish_date_from_prodviewdb(self,prodview_product_id):
        """获取产品publish_date from prodviewdb"""
        sql_get_product_id="select publish_date from products_core_search where product_id=%s;" % prodview_product_id[0]
        publish_date_list=self.getinfo_from_mysql(sql_get_product_id,self.host_search,self.user,self.passwd,self.db_search)
        return publish_date_list
    def get_publish_date_from_ProductDB(self,product_product_id):
        """获取产品publish_date from ProductDB"""
        sql_get_publish_date="select publish_date from Products_Core where product_id=%s;" % product_product_id
        publish_date_list=self.getinfo_from_mysql(sql_get_publish_date,self.host_search,self.user,self.passwd,self.db_search)
        return publish_date_list
    def update_publish_date_to_prodviewdb(self,product_id_list,publish_date):
        #修改搜索数据库prodviewdb.products_core_search的publish_date
        print "修改搜索数据库prodviewdb.products_core_search的publish_date"
        for i in product_id_list:
            update_publish_date_to_prodviewdb="update products_core_search set publish_date='%s' where product_id=%s;" % (publish_date,i)
            self.updateinfo_to_mysql(update_publish_date_to_prodviewdb,self.host_search,self.user,self.passwd,self.db_search)
    def update_publish_date_to_ProductDB(self,flag_null,product_id_list,publish_date):
        #修改单品库ProductDB.Products_Core的publish_date
        #对于出版时间（publish_date字段）不为NULL，且小于或等于“1910-01-01 00:00:00”，则更改此字段为“1970-01-01 00:00:00”
        print "修改单品库ProductDB.Products_Core的publish_date"
        if flag_null=="no":
            for i in product_id_list:
                update_publish_date_ProductDB="update Products_Core set publish_date=%s where product_id=%s;" % (publish_date,i)
                self.updateinfo_to_mysql(update_publish_date_ProductDB,self.host_product,self.user,self.passwd,self.db_product)
        elif flag_null=="yes":
            for i in product_id_list:
                update_publish_date_ProductDB="update Products_Core set publish_date=%s where product_id=%s;" % (publish_date,i)
                self.updateinfo_to_mysql(update_publish_date_ProductDB,self.host_product,self.user,self.passwd,self.db_product)
        elif flag_null=="all":
                update_publish_date_ProductDB_0001="update Products_Core set publish_date=%s where product_id=%s;" % (self.publish_date_0001,product_id_list[0])
                self.updateinfo_to_mysql(update_publish_date_ProductDB_0001,self.host_product,self.user,self.passwd,self.db_product)
                update_publish_date_ProductDB_NULL="update Products_Core set publish_date is %s where product_id=%s;" % ("NULL",product_id_list[1])
                self.updateinfo_to_mysql(update_publish_date_ProductDB_NULL,self.host_product,self.user,self.passwd,self.db_product)
                update_publish_date_ProductDB_small="update Products_Core set publish_date=%s where product_id=%s;" % (self.publish_date_small,product_id_list[2])
                self.updateinfo_to_mysql(update_publish_date_ProductDB_small,self.host_product,self.user,self.passwd,self.db_product)
                update_publish_date_ProductDB_equal="update Products_Core set publish_date=%s where product_id=%s;" % (self.publish_date_equal,product_id_list[3])
                self.updateinfo_to_mysql(update_publish_date_ProductDB_equal,self.host_product,self.user,self.passwd,self.db_product)
                update_publish_date_ProductDB_big="update Products_Core set publish_date=%s where product_id=%s;" % (self.publish_date_big,product_id_list[3])
                self.updateinfo_to_mysql(update_publish_date_ProductDB_big,self.host_product,self.user,self.passwd,self.db_product)
        else:
            pass
    def update_process_last_date_prodviewdb(self,process_last_date):
        #初始化process_last_date from 搜索数据库prodviewdb.process_status
        print "修改搜索数据库prodviewdb.process_status的process_last_date"
        update_process_last_date_to_prodviewdb="update process_status set process_last_date=%s where process_name = 'Dang.SearchDataArch.Service.ProductCoreSync';" % process_last_date
        self.updateinfo_to_mysql(update_process_last_date_to_prodviewdb,self.host_search,self.user,self.passwd,self.db_search)
    def update_last_changed_date_ProductDB(self,last_changed_date):
        #修改单品库中的ProductDB.Products_Core中的last_changed_date
        print "修改单品库中的ProductDB.Products_Core中的last_changed_date"
        for i in productId_list:    
            update_last_changed_date_ProductDB="update Products_Core set last_changed_date=%s where product_id=%s;" % (last_changed_date,i)
            self.updateinfo_to_mysql(update_last_changed_date_ProductDB,self.host_product,self.user,self.passwd,self.db_product)
    def get_same_product_id(self,prodview_product_id_list,product_product_id_list):
        """根据prodview库中product_id和ProductDB的交集"""
        set1=set(prodview_product_id_list)
        set2=set(product_product_id_list)
        set3=set1 & set2
        tmp=list(set3)
        product_id_list=tmp[0:5]
        return product_id_list
            
        
    def test_001_publish_date_is_NULL(self):
        print "*"*50
        """
        print "test_001_publish_date_is_NULL"
        prodview_product_id_list=self.get_product_id_list_from_prodviewdb()
        print "@",prodview_product_id_list
        
        product_product_id_list=self.get_product_id_list_from_ProductDB()
        print "@",product_product_id_list
        
        product_id_list=self.get_same_product_id(prodview_product_id_list, product_product_id_list)
        print product_id_list
        
        
        product_id_list=[2029465601L, 2029466501L, 2029466901L, 2015032605L, 2015032606L]
        
        
        publish_date_prodviewdb='1971-01-01 00:00:00'
        self.update_publish_date_to_prodviewdb(product_id_list,publish_date_prodviewdb)
        
        publish_date_list_view=self.get_publish_date_from_prodviewdb(product_id_list)
        
        str_time=publish_date_list_view[0].strftime("%Y-%m-%d %H:%M:%S")
        
        self.assertEqual(str_time,publish_date_prodviewdb)
        
        """
        
        
        publish_date_ProductDB="NULL"
        self.update_publish_date_to_ProductDB("yes",product_id_list,publish_date_ProductDB)
        
        
        """
        self.update_last_changed_date_ProductDB(self.last_changed_date)
        
        self.update_process_last_date_prodviewdb(self.process_last_date)
        
        #wait for ProductCoreSync
        sleep(60)
        
        #publish_date of prodviewdb.products_core_search should not be changed
        publish_date_list=self.get_publish_date_from_prodviewdb
        for i in publish_date_list:
            self.assertEqual(str(i),publish_date_prodviewdb)
        """
    def non_test_002_publish_date_smaller_than_19100101000000(self):
        print "*"*50
        print "test_002_publish_date_smaller_than_19100101000000"
        product_id_list=self.get_product_id_list
        
        publish_date_prodviewdb='1971-01-01 00:00:00'
        self.update_publish_date_to_prodviewdb(product_id_list,publish_date_prodviewdb)
        
        publish_date_ProductDB="1909-12-31 23:59:59"
        self.update_publish_date_to_ProductDB("no",product_id_list,publish_date_ProductDB)
     
        self.update_last_changed_date_ProductDB(self.last_changed_date)

        self.update_process_last_date_prodviewdb(self.process_last_date)
        
        #wait for ProductCoreSync
        sleep(60)
        
        #publish_date of prodviewdb.products_core_search should be changed
        
        publish_date_list=self.get_publish_date_from_prodviewdb
        for i in publish_date_list:
            self.assertEqual(str(i),self.publish_date)    
    def non_test_003_publish_date_equal_19100101000000(self):
        print "*"*50
        print "test_003_publish_date_equal_19100101000000"
        product_id_list=self.get_product_id_list
        
        publish_date_prodviewdb='1971-01-01 00:00:00'
        self.update_publish_date_to_prodviewdb(product_id_list,publish_date_prodviewdb)
        
        publish_date_ProductDB="1910-01-01 00:00:00"
        self.update_publish_date_to_ProductDB("no",product_id_list,publish_date_ProductDB)
     
        self.update_last_changed_date_ProductDB(self.last_changed_date)
 
        self.update_process_last_date_prodviewdb(self.process_last_date)
        
        #wait for ProductCoreSync
        sleep(60)
        
        #publish_date of prodviewdb.products_core_search should be changed
        publish_date_list=self.get_publish_date_from_prodviewdb
        for i in publish_date_list:
            self.assertEqual(str(i),self.publish_date)   
    def non_test_004_publish_date_equal_0001(self):
        print "*"*50
        print "test_004_publish_date_equal_0001"
        product_id_list=self.get_product_id_list
        
        publish_date_prodviewdb='1971-01-01 00:00:00'
        self.update_publish_date_to_prodviewdb(product_id_list,publish_date_prodviewdb)
        
        publish_date_ProductDB="0001 00:00:00"
        self.update_publish_date_to_ProductDB("no",product_id_list,publish_date_ProductDB)
    
        self.update_last_changed_date_ProductDB(self.last_changed_date)
        
        self.update_process_last_date_prodviewdb(self.process_last_date)
        
        #wait for ProductCoreSync
        sleep(60)
        
        #publish_date of prodviewdb.products_core_search should be changed
        publish_date_list=self.get_publish_date_from_prodviewdb
        for i in publish_date_list:
            self.assertEqual(str(i),self.publish_date)  
        
    def non_test_005_publish_date_bigger_then_19100101000000(self):
        print "*"*50
        print "test_005_publish_date_equal_19100101000000"
        product_id_list=self.get_product_id_list
        
        publish_date_prodviewdb='1971-01-01 00:00:00'
        self.update_publish_date_to_prodviewdb(product_id_list,publish_date_prodviewdb)
        
        publish_date_ProductDB="1910-01-01 00:00:01"
        self.update_publish_date_to_ProductDB("no",product_id_list,publish_date_ProductDB)
     
        self.update_last_changed_date_ProductDB(self.last_changed_date)
     
        self.update_process_last_date_prodviewdb(self.process_last_date)
        
        #wait for ProductCoreSync
        sleep(60)
        
        #publish_date of prodviewdb.products_core_search should not be changed
        publish_date_list=self.get_publish_date_from_prodviewdb
        for i in publish_date_list:
            self.assertEqual(str(i),publish_date_prodviewdb)
            
    def non_test_006_publish_date_smaller_equal_bigger_null_0001_then_19100101000000(self):
        print "*"*50
        print "test_006_publish_date_smaller_equal_bigger_null_0001_then_19100101000000"
        product_id_list=self.get_product_id_list
        
        publish_date_prodviewdb='1971-01-01 00:00:00'
        self.update_publish_date_to_prodviewdb(product_id_list,publish_date_prodviewdb)
        
        publish_date_ProductDB="1910-01-01 00:00:01"
        self.update_publish_date_to_ProductDB("all",product_id_list,publish_date_ProductDB)
     
        self.update_last_changed_date_ProductDB(self.last_changed_date)
       
        self.update_process_last_date_prodviewdb(self.process_last_date)
        
        #wait for ProductCoreSync
        sleep(60)
        
        #publish_date of prodviewdb.products_core_search should not be changed
        publish_date_list=self.get_publish_date_from_prodviewdb
        for i in publish_date_list:
            self.assertEqual(str(i),publish_date_prodviewdb)
        
if __name__ == "__main__":
    unittest.main()   


    










