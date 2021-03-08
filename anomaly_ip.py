#coding:utf-8
#!usr/bin/env python
'''
@author:lucas 1273085613@qq.com
@description:异常ip检测
@create date:2017/06/08
'''
from  fileTraff import file_path
from  fileTraff import read_file
from  fileTraff import file_name
from  DBTraff import db_base
from  DBTraff import db_config
import pandas as pd
import re
import sys
import time
import datetime
import os

def get_db():
    '''
    获取数据库中异常库，返回df
    '''
    return db_base.db_to_df('anomaly_detection','','protocol','port','name')

def getYesterday():
    '''
    获取昨天日期
    '''
    today=datetime.date.today()
    oneday=datetime.timedelta(days=1)
    yesterday=today-oneday
    return yesterday
def get_time_data():
    '''
    获取插入的时间数据
    '''
    time_data=''
    now_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    file_index=int(now_time[11:13])
    if file_index==0:
        time_data=str(getYesterday())
    else:
        time_data=now_time[0:10]
    return time_data

def get_file(filename):
    '''
    获取所需读取文件的绝对路径
    按小时读取.log的压缩文件
    '''
    file_path_all=''
    now_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    file_index=int(now_time[11:13])
    if file_index==0:
        file_path_all=file_path.zip_log_file_path+getYesterday()+'/'+file_name.get_name_list(filename)[23]
    else:
        file_path_all=file_path.zip_log_file_path+now_time[0:10]+'/'+file_name.get_name_list(filename)[file_index-1]
    return file_path_all

def get_log():
    '''
    获取log所需信息，返回df
    '''
    df=pd.DataFrame()
    file_path_all=get_file('conn')
    #file_path_all='test_file/conn.gz'
    if os.path.exists(file_path_all):
        log_df=read_file.pandas_normal_gz(file_path_all)
        df=log_df.iloc[:-1,[6,3,5,2]]
        df.rename(columns={6:'protocol',3:'orig_port',5:'resp_port',2:'orig_ip'},inplace = True)
    else:
        return df
    return df
def get_result_df():
    '''
    获取异常ip信息，返回df
    '''
    log_df=get_log()
    db_df=get_db()
    log_df.rename(columns={'resp_port':'port'},inplace=True)
    result_df1=pd.merge(log_df,db_df,on=['protocol','port'])
    log_df.rename(columns={'port':'resp_port','orig_port':'port'},inplace=True)
    result_df2=pd.merge(log_df,db_df,on=['protocol','port'])
    result_df=pd.concat([result_df1.iloc[:,[3,4]],result_df2.iloc[:,[3,4]]],ignore_index=True)
    result_df.drop_duplicates(['orig_ip','name'],inplace=True)
    return result_df
def get_database_df(date_time):
    '''
    获取date_time当天数据库中的数据
    返回dataframe,“ip，anomalyType”
    '''
    where="where date='%s'"%(date_time)
    df_result=db_base.db_to_df('anomaly_ip',where,'ip','anomalyType')
    df_result.rename(columns={'ip':'orig_ip','anomalyType':'name'},inplace=True)
    return df_result
def write_database():
    date_time=get_time_data()
    log_df=get_result_df()
    db_df=get_database_df(date_time)
    last_df=pd.concat([log_df,db_df],ignore_index=True)
    last_df.drop_duplicates(['orig_ip','name'],inplace=True)
    #删除数据库当天的数据；
    sql="delete from anomaly_ip where date='%s'"%(date_time)
    db_base.execute_no_result(sql)
    #写入结果；
    sql1="insert into anomaly_ip(date,ip,anomalyType) values"
    for line in last_df.values:
        ip=line[0]
        anomalyType=line[1]
        sql1=sql1+"('%s','%s','%s')," %(date_time,ip,anomalyType)
    sql1=sql1[:-1]
    db_base.execute_no_result(sql1)#批量插入
    
if __name__=="__main__":
    write_database()