





#coding:utf-8:
#!usr/bin/env python
'''
@author:leilf 
@description:mac analysis
@create date:2017/06/14
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
import path_config
import numpy as np
import path_config

def get_mac_table():
        s="select MAC,Organization from MAC_OUI"
        sql_result=db_base.execute_result(s)
        mac_result={}
        for i in sql_result:
                mac_result[i[0]]=i[1]
	#print mac_result
        return mac_result
#get_mac_table()
def mac_analysis_test():
	pattern=db_config.get_pattern()
        mac_addr=[]
	np_list=[]
        file_path_all=path_config.get_conn_file_path()
        #file_path_all='/usr/local/bro/logs/current/conn.log'
        if not os.path.exists(file_path_all):
                return pd.DataFrame()
        df=read_file.pandas_normal(file_path_all)
	df=df[df.iloc[:,2].str.match(pattern).fillna(False)]
        mac_device=df.iloc[:,[2,22]]#ip,resp_mac
        mac_device=mac_device.dropna(axis=0)
        mac_device_groupy=mac_device.groupby([2,22])
	db_mac=get_mac_table()
        for i in mac_device_groupy.groups:
		#print i
                mac_addr=i[1]
		ip=i[0]
		mac_addr_result1=mac_addr[0:2]
		mac_addr_result2=mac_addr[3:5]
		mac_addr_result3=mac_addr[6:8]
		mac_addr_result=mac_addr_result1+'-'+mac_addr_result2+'-'+mac_addr_result3
		mac_addr_result=mac_addr_result.upper()
        	if mac_addr_result in db_mac.iterkeys():
			mac_pingpai=db_mac[mac_addr_result]
			#print mac_pingpai
			np_list.append([ip,mac_addr,mac_pingpai])
		#print i[1]		
		#print mac_addr_result
		else:
			continue
	#print np_list
	df_result=pd.DataFrame(np_list,columns=['user','mac','pingpai'])
	return df_result

#mac_analysis_test()


def get_database_df():
	sql="select ip,MAC,mfrs from MAC_analysis"
	sql_result=db_base.execute_result(sql)
	np_list=[]
	for i in sql_result:
        	np_list.append([i[0],i[1],i[2]])
	df_result=pd.DataFrame(np_list,columns=['user', 'mac','pingpai'])
	return df_result

def write_database():
	database_df=get_database_df()
	logdata_df=mac_analysis_test()
	if len(logdata_df)==0:
        	return
	concat_df_group=pd.concat([logdata_df,database_df],ignore_index=True).groupby(['user','mac','pingpai'])
	sql="delete from MAC_analysis"
	db_base.execute_no_result(sql)
	sql1="insert into MAC_analysis(ip,MAC,mfrs) values"
	for gp in concat_df_group.groups:
		_user=gp[0]
        	_mac=gp[1]
        	_pingpai=gp[2]
        	sql1=sql1+"('%s','%s','%s')," %(_user,_mac,_pingpai)
	sql1=sql1[:-1]
	#print sql1
	db_base.execute_no_result(sql1)
write_database()














