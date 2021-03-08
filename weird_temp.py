#coding:utf-8:
#!usr/bin/env python
'''
@author:cs 
@description:ycjc
@create date:2018/12/11
'''

#from  fileTraff import file_path
from  fileTraff import read_file
from  fileTraff import file_name
import numpy as np
import pandas as pd
#import MySQLdb
import time
#from  DBTraff import db_base
#from  DBTraff import db_config
import os
import datetime
#import path_config

def getYesterday():
	'''
	获取昨天日期
	'''
	today=datetime.date.today()
	oneday=datetime.timedelta(days=1)
	yesterday=today-oneday
	return yesterday

def get_past_hour_ts(n_hour):
	'''
	获取过去n_hour个小时整点时间戳
	'''
	now_time=time.strftime('%Y-%m-%d %H',time.localtime(time.time()))
	ts=time.mktime(time.strptime(now_time,'%Y-%m-%d %H'))-3600*n_hour
	return ts

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

def get_yc_content():
	#file_path_all=path_config.get_file_path()
	file_path_all='C:\\Users\\qq\\Desktop\\weird.log'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal(file_path_all)
	df_drop=df.iloc[:,[0,2,4]]
	usagent_content=df_drop.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([0,2,4])

	yc_result=[]#ip
	for yc_ip in result.groups:
		time=yc_ip[0]
		ori_ip=yc_ip[1]
		res_ip=yc_ip[2]
		yc_result.append([time,ori_ip,res_ip])
	return yc_result


def get_yc_join():
	#file_path_all=path_config.get_file_path()
	file_path_all='C:\\Users\\qq\\Desktop\\conn.log'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal(file_path_all)
	df_drop=df.iloc[:,[0,2,4,17,19]]
	usagent_content=df_drop.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([0,2,4,17,19])
	yc_result_1=[]#ip
	for yc_ip in result.groups:
		time=yc_ip[0]
		ori_ip=yc_ip[1]
		res_ip=yc_ip[2]
		ori_k=yc_ip[3]
		res_k=yc_ip[4]
		yc_result_1.append([time,ori_ip,res_ip,ori_k,res_k])
	return yc_result_1


def get_list_inter():
	w_data=get_yc_content()
	c_data=get_yc_join()
	ori_k=0
	res_k=0
	list=[]
	time=0
	for i in w_data:
		time=i[0]
		ori_ip=i[1]
		res_ip=i[2]
		for y in c_data:
			time1=y[0]
			ori_ip1=y[1]
			res_ip1=y[2]
			ori_k=y[3]
			res_k=y[4]
			if time==time1:
				if ori_ip==ori_ip1 :
					if res_ip==res_ip1:
						ori_k+=ori_k
						res_k+=res_k
	list.append([time,ori_k,res_k])	
	return list


	
def get_database_df():
        '''
        获取date_time当天数据库中的数据
        返回dataframe
        '''
        #sql="select white,gray,black from anomaly_list where date='%s'"%(date_time)
	sql="truncate table anomaly_list"
	sql_result=db_base.execute_result(sql)
        #np_list=[]
        #for i in sql_result:
                #np_list.append([i[0],i[1],i[2]])
        #df_result=pd.DataFrame(np_list,columns=['user', 'app_name','duration'])
	return sql_result

def to_database():
	data=get_list_inter()
	sql="insert into  ana_temp(date,ori_k,res_k) values"
	for i in data:
		sql1=sql+"('%s'),"%(i[0],i[1],i[2])
		#print i[0]
	sql1=sql1[:-1]
	print sql1
	#db_base.execute_no_result(sql1)



        

#write_black()
to_database()
