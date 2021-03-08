#coding:utf-8
#!usr/bin/env python


'''
1.http取数据[1,12]
2.得到所需的文件id
3.对应conn.log取得流量大小
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
	
def get_http_video():
	file_path_all=get_file('http')
	#file_path_all='D:\\ipv4&6\\00-pyTraff\\test_file\\7_17_http.gz'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal_gz(file_path_all)
	df_drop=df.iloc[:,[0,1,12]]
	usagent_content=df_drop.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([1,12])#fid,user-agent-content
	
	app_use=[]#fid,content
	for app in result.groups:	
		fid=app[0]
		app_name=app[1]

		if  app_name.startswith('Mozilla') or  app_name.startswith('Dalvik')or app_name.startswith('Safari') or app_name.startswith('Opera'):
                        continue
	
		elif app_name.startswith('Youku') or app_name.startswith('youku-tudou') :
			app_use.append([fid,'Youku'])
		
		elif app_name.startswith('MGTV'):
			app_use.append([fid,'MGTV'])
			
		elif app_name.startswith('SOHUVideo'):
			app_use.append([fid,'SOHUVideo'])
			
		elif app_name.startswith('QYPlayer'):
			app_use.append([fid,'QYPlayer'])
			
		elif app_name.startswith('qqlive'):
			app_use.append([fid,'qqlive'])
			
		
                elif app_name.startswith('kwai'):
                        app_use.append([fid,'kwai'])
			
		elif app_name.startswith('PPStream'):
			app_use.append([fid,'PPStream'])
			
		elif app_name.startswith('Letv'):
			app_use.append([fid,'Letv'])
		
		elif app_name.startswith('Funshion'):
			app_use.append([fid,'Funshion'])
		
		elif app_name.startswith('Xfplay'):
			app_use.append([fid,'Xfplay'])
		
		else:
			continue
	df_app_use=pd.DataFrame(app_use,columns=['fid','video_name'])
	return df_app_use
	
def get_conn_content():
	file_path_all=get_file('conn')
	#file_path_all='D:\\ipv4&6\\00-pyTraff\\test_file\\7_11_conn.gz'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal_gz(file_path_all)
	df_drop=df.iloc[:,[1,2,17,19]]
	df_drop.columns=['fid','user','orig_ip_bytes','resp_ip_bytes']
	return df_drop


def get_reslut():
	http_content=get_http_video()
	conn_content=get_conn_content()
	pd_result = pd.merge(http_content, conn_content, on=['fid'])
	result=pd_result[['video_name','user','orig_ip_bytes','resp_ip_bytes']]
	return result


def get_database_df(date_time):
	'''
	'''
	sql="select video_name,user,orig_ip_bytes,resp_ip_bytes from video_flow_bytes where date='%s'"%(date_time)
	sql_result=db_base.execute_result(sql)
	np_list=[]
	for i in sql_result:
		np_list.append([i[0],i[1],i[2],i[3]])
	df_result=pd.DataFrame(np_list,columns=['video_name','user','orig_ip_bytes','resp_ip_bytes'])
	return df_result
	
def to_database():
	time_data=get_time_data()
	data=get_reslut()
	
	database_df=get_database_df(time_data)#数据库中的数据
	if len(data)==0:
		return 
	concat_df_group=pd.concat([data,database_df],ignore_index=True).groupby(['video_name','user'])#取值
	
	sql="delete from video_flow_bytes where date='%s'"%(time_data)#删除数据库的值
	db_base.execute_no_result(sql)
	sql1="insert into video_flow_bytes(date,video_name,user,orig_ip_bytes,resp_ip_bytes) values"
	for idata in concat_df_group.groups:
		_video_name=idata[0]
		_user=idata[1]
		#print concat_df_group.get_group(idata)
		#_duration=concat_df_group.get_group(idata).duration.sum()
		_orig_ip_bytes=concat_df_group.get_group(idata).orig_ip_bytes.sum()
		_resp_ip_bytes=concat_df_group.get_group(idata).resp_ip_bytes.sum()
		sql1=sql1+"('%s','%s','%s',%d,%d)," %(time_data,_video_name,_user,_orig_ip_bytes,_resp_ip_bytes)
	sql1=sql1[:-1]
	db_base.execute_no_result(sql1)#批量插入

	

def main():
	to_database()

if __name__=='__main__':
	main()
