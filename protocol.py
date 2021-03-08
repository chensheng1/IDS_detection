#coding:utf-8
#!usr/bin/env python
'''
@author:lucas 1273085613@qq.com
@description:统计各协议的流量和连接
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
    '''
    获取昨天日期
    '''
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=today-oneday  
    return yesterday
def get_file(filename):
    '''
    获取过去一小时文件
    '''
    file_path_all=''
    now_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    file_index=int(now_time[11:13])
    if file_index==0:
        file_path_all=file_path.zip_log_file_path+getYesterday()+'/'+file_name.get_name_list(filename)[23]
    else:
        file_path_all=file_path.zip_log_file_path+now_time[0:10]+'/'+file_name.get_name_list(filename)[file_index-1]
    return file_path_all
def get_data():
    '''
    获取所需数据
    '''
    file_path_all=get_file('conn')
    #file_path_all='conn.gz'
    if not os.path.exists(file_path_all):
        return 0
    else:
        all_data=read_file.pandas_normal_gz(file_path_all)
        value=all_data.iloc[:-1,[7,17,19]]#service,orig_bytes,resp_bytes
        useful_data=value.dropna(how='any') # 去掉包含缺失值的行
        useful_data.rename(columns={7:'service'},inplace = True)
        useful_data.rename(columns={17:'orig_bytes'},inplace = True)
        useful_data.rename(columns={19:'resp_bytes'},inplace = True)
        data=useful_data.groupby('service').sum()
        return data
def write_databae():
    data=get_data()
    if  not isinstance(data,pd.DataFrame):
        return 
    time_data=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    for i in list(data.index):
        sql1="insert into protocol(date,protocol,incomingTraff,outgoingTraff) values('%s','%s',%d,%d)" %(time_data,i,data.loc[i,'resp_bytes'],data.loc[i,'orig_bytes'])
        origin_ip=db_base.execute_result(sql1)
    
def main():
    #print get_data()
    write_databae()

if __name__=='__main__':
    main()