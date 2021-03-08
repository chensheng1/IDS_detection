#coding:utf-8
#!usr/bin/env python
'''
@author:llf
@description:文件类型累计排名，每一个小时统计一次
@create date:2017/06/23
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
	
	
def get_file_type():
	'''
	获取文件类型与累计出现次数
	返回一个df数据类型
	'''
	type=[]
	file_type_count=[]
	
	file_path_all=get_file('files')
	#file_path_all='D:\\ipv4&6\\00-pyTraff\\test_file\\12files.gz'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=read_file.pandas_normal_gz(file_path_all)
	files_content=df.iloc[:,[8]]#得到[file_type]
	files_content=files_content.dropna(axis=0)#过滤内容为空的
	z=files_content.groupby([8]).size().sort_values().tail(15)
	for i in z.index:
		type.append(i)
	for j in z.values:
		file_type_count.append(j)
	result=zip(type,file_type_count)
	df_type_result=pd.DataFrame(result,columns=['file_type','count1'])
	return df_type_result
	
def get_database_df(date_time):
	'''
	获取date_time当天数据库中的数据
	返回dataframe,“file_type，count”
	'''
	sql="select file_type,count1 from file_type where date='%s'"%(date_time)
	sql_result=db_base.execute_result(sql)
	np_list=[]
	for i in sql_result:
		np_list.append([i[0],i[1]])
	df_result=pd.DataFrame(np_list,columns=['file_type', 'count1'])
	return df_result			
		
def data_to_localDb():
	time_data=get_time_data()
	data=get_file_type()
	database_df=get_database_df(time_data)#数据库中的数据
	if len(data)==0:
		return 
	concat_df_group=pd.concat([data,database_df],ignore_index=True).groupby(['file_type'])#取值
	sql="delete from file_type where date='%s'"%(time_data)#删除数据库的值
	db_base.execute_no_result(sql)
	sql1="insert into file_type(date,file_type,count1) values"
	for idata in concat_df_group.groups:
		_file_type=idata
		
		_count1=concat_df_group.get_group(idata).count1.sum()
		sql1=sql1+"('%s','%s',%d)," %(time_data,_file_type,_count1)
	sql1=sql1[:-1]
	db_base.execute_no_result(sql1)#批量插入
		
def main():
	data_to_localDb()
if __name__=='__main__':
	main()
	
	
	
	
