#coding:utf-8
#!usr/bin/env python
'''
@author:llf
@description:设备排名预处理
@create date:2017/06/04
'''

from  fileTraff import file_path
from  fileTraff import read_file
from  fileTraff import file_name
import numpy as np
import pandas as pd
import MySQLdb
from  DBTraff import db_base
from  DBTraff import db_config
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
	ts=time.mktime(time.strptime(now_time,'%Y-%m-%d %H'))-3600*n_hour
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
		
def get_usagent_content():
	#file_path_all=get_file('http')
	file_path_all='/usr/local/data/http.log'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal(file_path_all)
	usagent_content=df.iloc[:,[0,2,12]]#得到[ip,usagent]
	usagent_content=usagent_content.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([0,2,12])#聚合[ip,usagent]

	device_type_result=[]
	for usa in result.groups:
		time_da=time.strftime("%Y--%m--%d %H:%M:%S",time.localtime(int(float(usa[0]))))
		ip=usa[1]
		usagent=usa[2]
		if len(usagent)>28:
			if usagent.startswith('Mozilla') or usagent.startswith('Safari') or usagent.startswith('Opera') or usagent.startswith('Dalvik') or usagent.startswith('User-Agent'):#常用user-agent
				#取得括号中内容
				if usagent.find('(')>0:
					kh_right=usagent.split('(')[1]
					kh_content=[kh_right.split(')')[0]]
					for i in kh_content:
						if i.startswith('X11'):
							if i.find('AMD64') or i.find('amd64') or i.find('ARM') or i.find('arm') or i.find('MIPS') or i.find('mips') or i.find('ppc') or i.find('sparc64') or i.find('i586') or i.find('i686') or i.find('x64') or i.find('x86') or i.find('x86_64'):
									device_type_result.append([time_da,ip,'Linux','Linux'])
						elif i.startswith('Windows') or i.startswith('compatible'):
							if i.find('Windows NT'):
								localtion_NT=i.find('Windows NT')
								localtion_FH=i.find(';',localtion_NT)
								if localtion_FH>0 and localtion_NT>0:
									Windows_version=i[localtion_NT:localtion_FH]
								else:
									continue
								device_type_result.append([time_da,ip,Windows_version,'Windows'])
							elif i.find('Windows 98'):
								device_type_result.append([time_da,ip,'Windows 98','Windows'])
							
						elif i.startswith('x86_64') or i.startswith('Macintosh'):
							device_type_result.append([time_da,ip,'Mac','IOS'])
						#IOS设备:
							#iPhone,iPad,Mac,
						elif i.startswith('iPhone'):
							localtion_hx1=i.find('_')
							localtion_hx2=i.find('_',localtion_hx1+1,localtion_hx1+3)
							if localtion_hx2>0 and localtion_hx1>0:
								iPhone_version='iPhone'+' '+i[localtion_hx1-2:localtion_hx2+2]
							elif localtion_hx2<0:
								iPhone_version='iPhone'+' '+i[localtion_hx1-2:localtion_hx1+2]
							else:
								continue
							device_type_result.append([time_da,ip,iPhone_version,'IOS'])
						elif i.startswith('iPad'):
							localtion_hx1=i.find('_')
							localtion_hx2=i.find('_',localtion_hx1+1,localtion_hx1+5)
							if localtion_hx2>0 and localtion_hx1>0:
								iPad_version='iPad'+' '+i[localtion_hx1-2:localtion_hx2+2]
							elif localtion_hx2<0:
								iPad_version='iPad'+' '+i[localtion_hx1-2:localtion_hx1+2]
							else:
								continue
							device_type_result.append([time_da,ip,iPad_version,'IOS'])
						elif i.startswith('compatible'):
							if i.find('Windows Phone')>0:
								device_type_result.append([time_da,ip,'Windows Phone','Windows'])
						elif i.startswith('Windows Phone'):	
							device_type_result.append([time_da,ip,'Windows Phone','Windows']) 
						elif i.startswith('Linux') and i.find('Android')>0:
							if len(i)>28:
								Andriod_version=i.split(';')
								if len(Andriod_version[-1])>9:
									Andriod_version_result=Andriod_version[-1]
								else:
									Andriod_version_result=Andriod_version[-2]
									
								Andriod_result_temp=Andriod_version_result.split(' ')
								Andriod_result=Andriod_result_temp[:-1]
								if len(Andriod_result)==4:
									Andriod_Linux_result=Andriod_result[1]+' '+Andriod_result[2]+' '+Andriod_result[3]
									device_type_result.append([time_da,ip,Andriod_Linux_result,'Android'])
								elif len(Andriod_result)==3:
									Andriod_Linux_result=Andriod_result[1]+' '+Andriod_result[2]
									device_type_result.append([time_da,ip,Andriod_Linux_result,'Android'])
								elif len(Andriod_result)==2:
									Andriod_Linux_result=Andriod_result[1]
									device_type_result.append([time_da,ip,Andriod_Linux_result,'Android'])
								else:
									continue
							else:
								continue
							
				else:
					
					continue#没括号
			else:
				continue#不是常用user-agent
	device_type_result
	#print df_device_type_result
	return device_type_result
			
		
def data_to_localDb():
	data=get_usagent_content()
	if len(data)==0:
		return 
	sql1="insert into device(date,user,device_type,device_class) values"
	for idata in data:
		_time=idata[0]
		_user=idata[1]
		_device_type=idata[2]
		_device_class=idata[3]
		sql1=sql1+"('%s','%s','%s','%s')," %(_time,_user,_device_type,_device_class)
	sql1=sql1[:-1]
	db_base.execute_no_result(sql1)#批量插入
		
def main():
	data_to_localDb()
	#get_usagent_content()
if __name__=='__main__':
	main()			
			
