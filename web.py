#coding:utf-8
#!usr/bin/env python
'''
@author:lucas 1273085613@qq.com
@description:获取用户上网信息
@create date:2017/04/13

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

def get_data_base_host():
    '''
    获取数据库中收录的网站host
    '''
    sql="select host,name from application_host"
    sql_result=db_base.execute_result(sql)
    result={}
    for i in sql_result:
        result[i[0].split('.')[0]]=i[1]
    return result

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
    file_index=int(now_time[11:13])
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
def get_logdata_df():
    '''
    获取所需写入数据库的数据
    返回dataframe,”用户，host，duration“
    '''
    pattern=db_config.get_pattern()#根据网络环境修改
    #file_path_all=get_file('http')
    file_path_all='/usr/local/data/http.log'
    if not os.path.exists(file_path_all):
        return pd.DataFrame()
    df=read_file.pandas_normal(file_path_all)
    df=df[df.iloc[:,2].str.match(pattern)]
    df=df.iloc[:-1,[0,2,8]]#ts,origIP,host
    df=df.dropna(how='any')
    grouped=df.groupby([0,2,8])
    db_host=get_data_base_host()#数据库中收录的网站host
    #print db_host
    np_list=[]
    for gp in grouped.groups:
        data_host=gp[2].split('.')
        if len(data_host)>1:
            if data_host[-2] in ['com','cn','net','gov','org']:
                data_host=data_host[-3]
            else:
                data_host=data_host[-2]
        if data_host in db_host.iterkeys():
            gp_df=grouped.get_group(gp)
            min_ts=gp_df.iloc[:,0].min()
            if min_ts<get_past_hour_ts(1):
                min_ts=get_past_hour_ts(1)            
            max_ts=gp_df.iloc[:,0].max()
            np_list.append([gp[0],gp[1],db_host[data_host]])
            np_list.append([gp[0],gp[1],db_host[data_host]])
    return np_list

def write_database():
	'''
	1.获取要当天数据库存在的数据df；
	2.获取从log中读取的df；
	3.合并df，并计算值得到结果；
	4.删除数据库当天的数据；
	5.写入结果；
	'''
	logdata_df=get_logdata_df()
	if len(logdata_df)==0:
		return
	sql1="insert into web(date,user,webhost) values"
	for gp in concat_df_group.groups:
		_time=gp[0]
		_user=gp[1]
		_webhost=gp[2]
		sql1=sql1+"('%s','%s','%s')," %(_time,_user,_webhost)
	sql1=sql1[:-1]
	db_base.execute_no_result(sql1)#批量插入
def main():
    write_database()
    #get_logdata_df()
#    file_path_all='test_file/http.gz'
#    df=read_file.pandas_normal_gz(file_path_all)
#    df_list=set(df.iloc[:,2])
#    #print len(df_list)
if __name__=='__main__':
    main()
