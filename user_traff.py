#coding:utf-8
#!usr/bin/env python
'''
@author:lucas 1273085613@qq.com
@description:获取用户相关信息
日期，ip，上网起止时间，上下行流量
@create date:2017/04/13

'''
from  fileTraff import file_path
from  fileTraff import read_file
from  fileTraff import file_name
from  DBTraff import db_base
from DBTraff import db_config
import pandas as pd
import sys
import time
import pickle
import datetime
import os

class traffUser(object):
    '''
    在流量中识别出的用户类
    '''
    def __init__(self,ip,min_ts,max_ts,incoming,outgoing):
        self.ip=ip
        self.min_ts=min_ts
        self.max_ts=max_ts
        self.incoming=incoming
        self.outgoing=outgoing
    def get_ip(self):
        return self.ip
    def get_min_ts(self):
        format_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(self.min_ts))
        return format_time
    def get_max_ts(self):
        format_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(self.max_ts))
        return format_time        
    def get_incoming(self):
        return self.incoming
    def get_outgoing(self):
        return self.outgoing
    def get_all(self):
        return self.ip,self.get_min_ts(),self.get_max_ts(),self.incoming,self.outgoing
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
def get_file(filename):
    '''
    获取所需读取文件的绝对路径
    按小时读取conn.log的压缩文件
    '''
    file_path_all=''
    now_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    file_index=int(now_time[11:13])
    if file_index==0:
        file_path_all=file_path.zip_log_file_path+getYesterday()+'/'+file_name.get_name_list(filename)[23]
    else:
        file_path_all=file_path.zip_log_file_path+now_time[0:10]+'/'+file_name.get_name_list(filename)[file_index-1]
    return file_path_all
def get_user_data():
    '''
    获取所需写入数据库的数据
    时间，originIP，流量开始时间，流量最后出现时间，上行流量，下行流量
    '''
    file_path_all=get_file('conn')
    #file_path_all='test_file/conn.gz'
    if not os.path.exists(file_path_all):
        return pd.DataFrame()
    df=read_file.pandas_normal_gz(file_path_all)
    df=db_config.filter_ip_df(df,2)
    df=df.iloc[:-1,[0,2,9,10]]#ts,origIP,origByte,respByte
    df=df.dropna(how='any')
    grouped=df.groupby(2)
    user_list=[]
    for gp in grouped.groups:
        gp_df=grouped.get_group(gp).iloc[:,[0,2,3]]
        min_ts=gp_df.iloc[:,0].min()
        max_ts=gp_df.iloc[:,0].max()
        outgoing=gp_df.iloc[:,1].sum()
        incoming=gp_df.iloc[:,2].sum()
        gpu=traffUser(gp,min_ts,max_ts,incoming,outgoing)
        user_list.append(gpu)
    return user_list
def write_database():
    user_data_out=get_user_data()#待插入的数据
    if len(user_data_out)==0:
        return 
    time_data=get_time_data()
    sql0="select date,user,beginTime,endTime,outgoingTraff,incomingTraff from user_traff where date='%s'"%(time_data)
    user_iterms=db_base.execute_result(sql0)    
    user_dict={}#已经存入的数据
    if len(user_iterms)>0:
        for user_iterm in user_iterms:
            user_dict[user_iterm[1]]=[user_iterm[2],user_iterm[3],user_iterm[5],user_iterm[4]]
    #########################
    user_data_insert=[]#需插入的数据
    user_data_update=[]#需更新的数据
    update_ip_list=[]
    for user in user_data_out:
        if user.get_ip() not in user_dict.iterkeys():
            user_data_insert.append(user)
        else:
            update_ip_list.append(user.get_ip())
            user.min_ts=user_dict[user.get_ip()][0]
            user.incoming=user.incoming+user_dict[user.get_ip()][2]
            user.outgoing=user.outgoing+user_dict[user.get_ip()][3]
            user_data_update.append(user)
    ################################
    if user_data_insert!=[]:
        sql1="insert into user_traff(date,user,beginTime,endTime,outgoingTraff,incomingTraff) values"
        for user in user_data_insert:
            sql1=sql1+"('%s','%s','%s','%s',%d,%d)," %(time_data,user.get_ip(),user.get_min_ts(),user.get_max_ts(),user.get_outgoing(),user.get_incoming())
        sql1=sql1[:-1]
        db_base.execute_no_result(sql1)#批量插入
    if user_data_update!=[]:
        sql2="delete from user_traff where user in ("
        for ip in update_ip_list:
            sql2=sql2+"'%s'," %(ip)
        sql2=sql2[:-1]+") and date='%s'"%(time_data)
        db_base.execute_no_result(sql2)#批量删除
        sql3="insert into user_traff(date,user,beginTime,endTime,outgoingTraff,incomingTraff) values"
        for user in user_data_update:
            sql3=sql3+"('%s','%s','%s','%s',%d,%d)," %(time_data,user.get_ip(),user.min_ts,user.get_max_ts(),user.get_outgoing(),user.get_incoming())
        sql3=sql3[:-1]            
        db_base.execute_no_result(sql3)#再次批量插入
    
def main():
    write_database()
if __name__=='__main__':
    main()