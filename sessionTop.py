#coding:utf-8
#!usr/bin/env python
'''
@author:lucas 1273085613@qq.com
@description:统计会话流量top10
@create date:2017/04/27

'''
from  fileTraff import file_path
from  fileTraff import read_file
from  fileTraff import file_name
from  DBTraff import db_base
from  DBTraff import db_config
import pandas as pd
import sys
import time
import datetime
import os

def getYesterday(): 
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=today-oneday  
    return yesterday
def get_file():
    '''
    获取过去一小时文件
    '''
    file_path_all=''
    now_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    file_index=int(now_time[11:13])
    if file_index==0:
        file_path_all=file_path.zip_log_file_path+getYesterday()+'/'+file_name.get_name_list('conn')[23]
    else:
        file_path_all=file_path.zip_log_file_path+now_time[0:10]+'/'+file_name.get_name_list('conn')[file_index-1]
    return file_path_all
def get_data():
    '''
    获取所需有效日志文件数据
    @return ：dataframe
    '''
    #调取read_file模块函数读取文件
    #df_conn=read_file.pandas_normal('conn.log')
    file_all_path=get_file()
    df_conn=read_file.pandas_normal_gz(file_all_path)
    df_conn.rename(columns={2:'orignIp',4:'respIp',17:'orign',19:'resp'},inplace=True)
    #获取所需要的信息
    df_conn_useful=df_conn.iloc[:-1,[2,4,17,19]]#uid,orig_ip_bytes,resp_ip_bytes
    df_all=df_conn_useful.groupby(['orignIp','respIp']).sum()
    df_all['results']=df_all['orign']+df_all['resp']
    df_results=df_all.sort_values(by='results',ascending=False).head(10)
    return df_results
def write_database():
    '''
    结果写入数据库
    '''
    sql_clear="delete from sessionTop"
    db_base.execute_no_result(sql_clear)#清空原有数据
    data=get_data()
    for index in data.index:
        sql_insert="insert into sessionTop(origIp,respIp,origBytes,respBytes) values('%s','%s',%d,%d)"%(index[0],index[1],data.loc[index,'orign'],data.loc[index,'resp'])
        db_base.execute_no_result(sql_insert)
if __name__=='__main__':
    write_database()