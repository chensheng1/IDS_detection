#coding:utf-8:
#!usr/bin/env python
'''
@author:cs 
@description:ycjc
@create date:2018/12/11
'''

from  fileTraff import file_path
from  fileTraff import read_file
from  fileTraff import file_name
import numpy as np
import pandas as pd
from sklearn.neighbors import LocalOutlierFactor
from scipy import stats
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
	file_path_all='C:\\Users\\qq\\Desktop\\conn.log'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal(file_path_all)
	df_drop=df.iloc[:,[8,9,10,16,17,18,19]]
	usagent_content=df_drop.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([8,9,10,16,17,18,19])
	yc_result=[]#ip
	for i in result.groups:
		a=i[0]
		b=i[1]
		c=i[2]
		d=i[3]
		e=i[4]
		f=i[5]
		g=i[6]
		yc_result.append([a,b,c,d,e,f,g])
	res=np.array(yc_result)
	return res

def get_ip():
	#file_path_all=path_config.get_file_path()
	file_path_all='C:\\Users\\qq\\Desktop\\conn.log'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal(file_path_all)
	df_drop=df.iloc[:,[0,2,4]]
	usagent_content=df_drop.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([0,2,4])
	ip_result=[]#ip
	for i in result.groups:
		a=i[0]
		b=i[1]
		c=i[2]
		ip_result.append([a,b,c])
	ip=np.array(ip_result)
	return ip

def loftest():
	X_train=get_yc_content()
	data=get_ip()
	clf = LocalOutlierFactor(n_neighbors=20, contamination=0.1)
	y_pred = clf.fit_predict(X_train)
	#print y_pred
	scores_pred = clf.negative_outlier_factor_
	threshold = stats.scoreatpercentile(scores_pred, 100 * 0.1)  # 根据异常样本比例，得到阈值，用于绘图
	count=0
	sum1=0
	sum2=0
	sum=0
	list=[]
	for i in xrange(len(y_pred)):
		if y_pred[i]==-1:
			count+=1
			sum1+=X_train[i][4]
			sum2+=X_train[i][6]
			list1=data[i].tolist()
			list1.append(scores_pred[i])
			list.append(list1)
	sum=sum1+sum2
	
	time_data=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
	sql1="insert into abnormal_flow(time,conn,highflow,lowflow,flow) values"
	sql1=sql1+"('%s',%d,%d,%d,%d)," %(time_data,count,sum1,sum2,sum)
	sql1=sql1[:-1]
	#print sql1
	db_base.execute_no_result(sql1)#批量插入
	
	sql2="insert into abnormal_score(date,ori_ip,res_ip,score) values"
	for j in list:
		time1=time.strftime("%Y--%m--%d %H:%M:%S",time.localtime(int(float(j[0]))))
		ori_ip=j[1]
		res_ip=j[2]
		score=j[3]
		sql2=sql2+"('%s','%s','%s',%d),"%(time1,ori_ip,res_ip,score)
		#print j
	sql2=sql2[:-1]
	#print sql2
	db_base.execute_no_result(sql2)


if __name__=='__main__':
	loftest()
	
