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
	#file_path_all=path_config.get_file_path()
	file_path_all='/usr/local/data/http.log'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal(file_path_all)
	usagent_content=df.iloc[:,[0,2,12]]#得到[ip,usagent]
	usagent_content=usagent_content.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([0,2,12])#聚合[ip,usagent]

	browser=[]
	for usa in result.groups:
		time_da=usa[0]
		ip=usa[1]
		usagent=usa[2]
		if len(usagent)>28:
			if 'LBBrowser' in usagent or 'LBBROWSER ' in usagent:
				browser.append([time_da,ip,'liebao'])
			elif 'QQBrowser' in usagent:
				browser.append([time_da,ip,'QQBrowser'])
			elif 'Avant Browser' in usagent:
				browser.append([time_da,ip,'Avant'])
			elif 'UCBrowser' in usagent  or 'UCWEB' in usagent:
				browser.append([time_da,ip,'UCbrowser'])
			elif 'Maxthon' in usagent :
				browser.append([time_da,ip,'Maxthon'])
			elif 'TencentTraveler 4.0' in usagent:
				browser.append([time_da,ip,'Tencent TT'])
			elif 'sogoumobilebrowser' in usagent:
				browser.append([time_da,ip,'sogou browser'])
			elif '360SE' in usagent or '360browser' in usagent:
				browser.append([time_da,ip,'360Browser'])
			elif 'Firefox' in usagent or 'firefox'in usagent:
				browser.append([time_da,ip,'Firefox'])
			elif 'Chrome'in usagent and 'Safari' in usagent:
				browser.append([time_da,ip,'Chrome'])
			elif 'MobileSafari'in usagent :
				browser.append([time_da,ip,'Safari'])
			elif 'macintosh'in usagent or 'Macintosh'in usagent  and 'Safari'in usagent:
				browser.append([time_da,ip,'Safari'])
			elif 'Opera'in usagent:
				browser.append([time_da,ip,'Opera'])
			elif 'MSIE' in usagent or 'mise'in usagent:
				browser.append([time_da,ip,'Internet Explorer'])
			elif 'Mb2345Browser' in usagent:
				browser.append([time_da,ip,'2345Browser'])
			elif 'Silk'in usagent:
				browser.append([time_da,ip,'Silk'])
			elif 'baidubrowser'in usagent:
				browser.append([time_da,ip,'BaiDuBrowser'])
			elif 'YaBrowser'in usagent:
				browser.append([time_da,ip,'YaBrowser'])	
			else :
				continue
		else:
			continue#不是常用user-agent
	#print browser
	return browser
			
		
def data_to_localDb():
	data=get_usagent_content()
	if len(data)==0:
		return 
	sql1="insert into browser(date,user,browser) values"
	for idata in data:
		_time=idata[0]
		_user=idata[1]
		_browser=idata[2]
		sql1=sql1+"('%s','%s','%s')," %(_time,_user,_browser)
	sql1=sql1[:-1]
	db_base.execute_no_result(sql1)#批量插入
		
def main():
	data_to_localDb()
if __name__=='__main__':
	main()			
			
