#coding:utf-8
#!usr/bin/env python
'''
@author：cs
@description:用户文件下载排名
@create date:2017/06/23
'''
from  fileTraff import file_path
from  fileTraff import read_file
from  fileTraff import file_name
import numpy as np
import pandas as pd
import time
import os 
import datetime


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
    file_index=int(now_time[11:13])#获取小时值
    if file_index==0:
        time_data=str(getYesterday())
    else:
        time_data=now_time[0:10]
    return time_data
	
def get_past_hour_ts(n_hour):
    '''
    获取过去n_hour个小时整点时间戳
    '''
    now_time=time.strftime('%Y-%m-%d %H',time.localtime(time.time()))
    ts=time.mktime(time.strptime(now_time,'%Y-%m-%d %H'))-60*n_hour
    return ts
	
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
	

	
def get_file_anomaly():
	'''
	根据file.log得到所需字段以df的数据类型返回
	
	'''
	#file_path_all=get_file('files')
	file_path_all='C:\\Users\\qq\\Desktop\\weird.log'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal(file_path_all)
	files_content=df.iloc[:,[0,2,3,4,5,6]]#得到[files_id,ori_ip,resp_ip,type,hash]
	files_content=files_content.dropna(axis=0)#过滤内容为空的
	result=files_content.groupby([0,2,3,4,5,6])#
	list_result=[]
	for i in result.groups:
		time_da=time.strftime("%Y--%m--%d %H:%M:%S",time.localtime(int(float(i[0]))))
		ori_ip=i[1]
		ori_k=int(i[2])
		resp_ip=i[3]
		resp_k=int(i[4])
		ana_be=i[5]
		list_result.append([time_da,ori_ip,ori_k,resp_ip,resp_k,ana_be])
	return list_result
		
def data_to_localDb():
	data=get_file_anomaly()
	if len(data)==0:
		return 
	sql1="insert into weird(start,ori_ip,ori_k,res_ip,resp_k,ana_be) values"
	for i in data:
		start_time=i[0]
		orig_ip=i[1]
		orig_k=i[2]
		resp_ip=i[3]
		resp_k=i[4]
		becu=i[5]
		sql1=sql1+"('%s','%s','%s','%s','%s','%s')," %(start_time,orig_ip,orig_k,resp_ip,resp_k,becu)
	sql1=sql1[:-1]
	db_base.execute_no_result(sql1)#批量插入
	
	
def main():
	data_to_localDb()
if __name__=='__main__':
	main()
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
