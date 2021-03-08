#coding:utf-8
#!usr/bin/env python
'''
@author:lucas 1273085613@qq.com
@description:实时统计
@create date:2017/04/09

'''
from  fileTraff import file_path
from  fileTraff import read_file
from  fileTraff import file_name
from  DBTraff import db_config

import pandas as pd
import sys
import os
import time
import datetime

def get_file(filename):
    '''
    获取文件路径
    '''
    file_path_all=file_path.current_log_file_path+filename
    return file_path_all
def read_file_now(filename):
    '''
    读取文件
    @param :文件完整路径
    '''
    while not os.path.exists(filename):
        time.sleep(0.5)
    return read_file.pandas_normal(filename)
        
def get_data(sleeptime):
    '''
    获取所需有效日志文件数据
    @return ：dataframe
    '''
    #调取read_file模块函数读取文件
    conn_log=get_file('conn.log')
    #conn_log='conn.log'
    
    df_conn1=read_file_now(conn_log)
    time.sleep(sleeptime)
    df_conn2=read_file_now(conn_log)
    #获取所需要的信息
    value1=df_conn1[0].count()#连接数
    value2=df_conn2[0].count()
    value3=df_conn1[17].sum()#源流量
    value4=df_conn2[17].sum()
    value5=df_conn1[19].sum()#目标流量
    value6=df_conn2[19].sum()
    if value2<value1:
        value1=0
    if value4<value3:
        value3=0
    if value6<value5:
        value5=0
    value7=value3+value5
    value8=value4+value6    
    return str(value1),str(value2),str(value3),str(value4),str(value5),str(value6),str(value7),str(value8)
    
def clear_table():
    #连接数据库并写入
    db=db_config.get_connected_db()
    cursor=db.cursor()
    sql0="delete from temp_all"
    try:
        # 执行sql语句
        cursor.execute(sql0)
        #results=cursor.fetchall()
        #print results
        # 提交到数据库执行
        db.commit()
    except:
        # Rollback in case there is any error
        db.rollback()
    
    # 关闭数据库连接
    db.close()
    
def write_database():
    '''
    写数据
    '''
    value1,value2,value3,value4,value5,value6,value7,value8=get_data(8)
    
    #连接数据库并写入
    clear_table()

    db=db_config.get_connected_db()
    cursor=db.cursor()        
    sql1 = "insert into temp_all(name,valuePre,valuePost) values('main_conn','"+value1+"','"+value2+"')"
    sql2 = "insert into temp_all(name,valuePre,valuePost) values('orig_traff','"+value3+"','"+value4+"')"
    sql3 = "insert into temp_all(name,valuePre,valuePost) values('resp_traff','"+value5+"','"+value6+"')"
    sql4 = "insert into temp_all(name,valuePre,valuePost) values('main_traff','"+value7+"','"+value8+"')"
    try:
        # 执行sql语句
        cursor.execute(sql1)
        cursor.execute(sql2)
        cursor.execute(sql3)
        cursor.execute(sql4)
        #results=cursor.fetchall()
        #print results
        # 提交到数据库执行
        db.commit()
    except:
        # Rollback in case there is any error
        db.rollback()
        
    # 关闭数据库连接
    db.close()

if __name__=='__main__':
    #print get_data()
    write_database()
    
