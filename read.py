#_*_coding:utf-8 _*_


import pandas as pd
import numpy as np
import MySQLdb
from  DBTraff import db_base
from  DBTraff import db_config

def get_database_df():
	'''
	获取date_time当天数据库中的数据
	返回dataframe
	'''
	sql="select * from train "
	sql_result=db_base.execute_result(sql)
	df=pd.DataFrame(list(sql_result))
	np_list=[]
	user_list=[]
	mp={'tcp':1,'udp':2,'ssl':3,'http':1,'dns':2,'dhcp':3,'dtls':4,'irc':5,'xmpp':6}
	df=df.replace(mp)
	sql_result=df.values
	for i in sql_result:
		pro=i[3]
		service=i[4]
		if i[10]==None or i[10]=='Null':
			browser=0
		else:
			browser=1
		if i[11]==None or i[11]=='Null':
			web=0
		else:
			web=1
		if i[12]==None or i[12]=='Null':
			app=0
		else:
			app=1
		if i[13]==None or i[13]=='Null':
			result.append(0)
		else:
			result.append(10)
		np_list.append([[pro,service,i[5],i[6],i[7],i[8],i[9],browser,web,app],[result]])
		user_list.append([[i[0],i[1],i[2]],[pro,service,i[5],i[6],i[7],i[8],i[9],browser,web,app]])
	return np_list,user_list
	
	

if __name__ == '__main__':
	print get_database_df()