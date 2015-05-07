#!/bin/python
#coding=utf-8
import sys   
reload(sys)  
sys.setdefaultencoding('utf8')  
import MySQLdb
import urllib2
import json
host_search="10.255.254.22"
host_product="10.255.255.22"
db_search="prodviewdb"
db_product="ProductDB"
productId_list=[]

def update_mysql(sql_cmd,host,db):
    conn=MySQLdb.connect(host=host,user="writeuser",passwd="ddbackend",db=db,charset="utf8")
    cursor = conn.cursor()
    num=5
    n = cursor.execute(sql_cmd)
    conn.commit()
    cursor.close()
    conn.close()
def get_mysql(sql_cmd,host,db):
    conn=MySQLdb.connect(host=host,user="writeuser",passwd="ddbackend",db=db,charset="utf8")
    cursor = conn.cursor()
    #num=5
    #sql_cmd="select product_id from products_core_search order by product_id desc limit %s;" % num
    #sql_cmd="select product_id,publish_date from products_core_search order by product_id desc limit %s;" % num
    n = cursor.execute(sql_cmd)
    for row in cursor.fetchall():
        for i in row:    
            productId_list.append(int(i))
    cursor.close()
    #cursor.commit()
    conn.close()

#获取产品id

num=5
print "获取产品id"
sql_get_product_id="select product_id from products_core_search order by product_id desc limit %s;" % num
get_mysql(sql_get_product_id,host_search,db_search)
print "产品id为："
print productId_list


#修改搜索数据库prodviewdb.products_core_search的publish_date
print "修改搜索数据库prodviewdb.products_core_search的publish_date"
for i in productId_list:
    update_publish_date_cmd_search="update Products_Core set publish_date='1971-01-01 00:00:00' where product_id=%s;" % i
    update_mysql(update_publish_date_cmd_search,host_search,db_search)
    

#修改单品库ProductDB.Products_Core的publish_date
#对于出版时间（publish_date字段）不为NULL，且小于或等于“1910-01-01 00:00:00”，则更改此字段为“1970-01-01 00:00:00”
print "修改单品库ProductDB.Products_Core的publish_date"
for i in productId_list:
    update_publish_date_cmd_product="update Products_Core set publish_date='1900-12-31 23:59:59' where product_id=%s;" % i
    update_mysql(update_publish_date_cmd_product,host_product,db_product)


#初始化process_last_date from 搜索数据库prodviewdb.process_status
print "修改搜索数据库prodviewdb.process_status的process_last_date"
update_publist_date_cmd="update process_status set process_last_date='2015-05-07 00:00:00' where process_name = 'Dang.SearchDataArch.Service.ProductCoreSync' ";
update_mysql(update_publist_date_cmd,host_search,db_search)


#修改单品库中的ProductDB.Products_Core中的last_changed_date
print "修改单品库中的ProductDB.Products_Core中的last_changed_date"
for i in productId_list:    
    update_last_changed_date_cmd="update Products_Core set last_changed_date='2015-05-07 00:01:00' where product_id=%s;" % i
    update_mysql(update_last_changed_date_cmd,host_product,db_product)


