# -*- coding: utf-8 -*-


import MySQLdb
from  DBTraff import db_base
from  DBTraff import db_config
import BPANN
import read
# Demo Function


def main():

	# 测试数据
	data=read.get_database_df()[0]
	data2=read.get_database_df()[1]
	
	# 实例化神经网络类
	ann = BPANN.BPNeuralNetworks(10, 13, 1)
	ann.train(data, iterations = 700)
	list=ann.caclulate(data2)
	sql1="insert into bp_abnormal(date,ori_ip,resp_ip,abnormal,ab) values"
	for i in list:
		date1=i[0][0]
		ori=i[0][1]
		res=i[0][2]
		abnormal=i[1]
		if float(i[1])>0.5:
			ab='异常'
		else:
			ab='正常'
		sql1=sql1+"('%s','%s','%s',%d,'%s')," %(date1,ori,res,abnormal,ab)
	sql1=sql1[:-1]
	db_base.execute_no_result(sql1)#批量插入

if __name__ == '__main__':
	main()