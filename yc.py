#coding:utf-8:
#!usr/bin/env python
'''
@author:leilf 
@description:ycjc
@create date:2018/12/11
'''

from  fileTraff import file_path
from  fileTraff import read_file
from  fileTraff import file_name
import numpy as np
import pandas as pd
import MySQLdb
import time
from  DBTraff import db_base
from  DBTraff import db_config
import os
import datetime
import path_config

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

def get_yc_content():#black
	#file_path_all=path_config.get_file_path()
	file_path_all='/usr/local/data/weird.log'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal(file_path_all)
	df_drop=df.iloc[:,[2,6]]
	usagent_content=df_drop.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([2,6])

	yc_result=[]#ip
	for yc_ip in result.groups:
		ip=yc_ip[0]
		yc_tz=yc_ip[1]
		if yc_tz.startswith('connection'):
			yc_result.append([ip])
	#dataa=str(yc_result)
	#dataa=dataa.replace('[','')
        #dataa=dataa.replace(']','')
        #a=list(eval(dataa))
	#print dataa
	return yc_result


def get_yc_join():#wihte gray
	#file_path_all=path_config.get_file_path()
	file_path_all='/usr/local/data/conn.log'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal(file_path_all)
	df_drop=df.iloc[:,[2]]
	usagent_content=df_drop.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([2])
	yc_result_1=[]#ip
	for yc_ip in result.groups:
		yc_result_1.append([yc_ip])
	#print yc_result_1
	#print len(yc_result_1)
	return yc_result_1

def get_yc_sub():#wihte gray
	#file_path_all=path_config.get_file_path()
	file_path_all='/usr/local/data/weird.log'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal(file_path_all)
	df_drop=df.iloc[:,[2]]
	usagent_content=df_drop.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([2])
	yc_result_2=[]#ip
	for yc_ip in result.groups:
		yc_result_2.append([yc_ip])
	#print (yc_result_2)
	return yc_result_2

def get_list_inter():
	dataa=str(get_yc_sub())
	datab=str(get_yc_join())
	dataa=dataa.replace('[','')
	dataa=dataa.replace(']','')
	datab=datab.replace('[','')
	datab=datab.replace(']','')
	a=list(eval(dataa))
	b=list(eval(datab))
	c=set(a).intersection(set(b))
	d=set(a).difference(set(b))
	#print c
	return [c,d]

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
        #time_data=get_time_data()
        data=get_yc_content()
	#print data
	data2=get_list_inter()[0]
	data3=get_list_inter()[1]
	get_database_df()#数据库中的数据
	sql="insert into  white(wh) values"
	for i in data:
		sql1=sql+"('%s'),"%(i[0])
		#print i[0]
	sql1=sql1[:-1]
	#print sql1
	db_base.execute_no_result(sql1)

	sqll="insert into gray(gray) values"
	for j in data2:
		sql2=sqll+"('%s'),"%(j)
		#print j
	sql2=sql2[:-1]
	print sql2
	db_base.execute_no_result(sql2)

def write_black():	
	data3=get_list_inter()[1]        
	sql2="insert into  black(black) values"
	for j in data3:
		sql2=sql2+"('%s'),"%(j)
		#print j
	sql2=sql2[:-1]
	print sql2
	db_base.execute_no_result(sql2)



        

#write_black()
to_database()
