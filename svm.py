#!/usr/bin/python
#coding=utf-8


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from sklearn import svm
from sklearn.metrics import accuracy_score
from sklearn import metrics
import csv
import pandas as pd
import numpy as np
import os
import sys
import urllib.request
import datetime
import read

def readfile():
	file_path_all='C:\\Users\\qq\\Desktop\\train5.csv'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=pd.read_csv(file_path_all,header=None)
	df_drop=df.iloc[:,[5,6,7,8,9,10,11,12]]
	usagent_content=df_drop.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([5,6,7,8,9,10,11,12])
	yc_result=[]#ip
	result_a=[]
	for i in result.groups:
		a=i[0]
		b=i[1]
		c=i[2]
		d=i[3]
		e=i[4]
		f=i[5]
		g=i[6]
		h=i[7]
		yc_result.append([a,b,c,d,e,f,g])
		result_a.append(h)
	data=np.array(yc_result)
	target=np.array(result_a)
	return data,target

def readfile1():
	file_path_all='C:\\Users\\qq\\Desktop\\train5.csv'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=pd.read_csv(file_path_all,header=None)
	df_drop=df.iloc[:,[5,6,7,8,9,10,11,12]]
	usagent_content=df_drop.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([5,6,7,8,9,10,11,12])
	yc_result=[]#ip
	result_a=[]
	n=0
	for i in result.groups:
		a=i[0]
		b=i[1]
		c=i[2]
		d=i[3]
		e=i[4]
		f=i[5]
		g=i[6]
		h=i[7]
		if h==0 and n<int(len(result)*0.59):
			n=n+1
			continue
		else:
			yc_result.append([a,b,c,d,e,f,g])
			result_a.append(h)
	data=np.array(yc_result)
	target=np.array(result_a)
	return data,target	
	
def readfile2():
	file_path_all='C:\\Users\\qq\\Desktop\\zdy.csv'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=pd.read_csv(file_path_all,header=None)
	columns=df.iloc[:,[4,5,6,8,9,10,11]]
	columns=columns.values
	type=df.iloc[:,[13]]
	type=type.values
	data=np.array(columns)
	target=np.array(type)
	return data,target

def readfile3():
	file_path_all='C:\\Users\\qq\\Desktop\\zdy.csv'
	if not os.path.exists(file_path_all):
		return pd.DataFrame()
	df=pd.read_csv(file_path_all,header=None)
	columns=df.iloc[:,[4,5,6,8,9,10,11]]
	columns=columns.values
	type=df.iloc[:,[13]]
	type=type.values
	data=np.array(columns)
	target=np.array(type)
	return data,target		
	
def main():
	clf = svm.SVC(C=1, kernel='rbf', gamma='auto',tol=0.001, decision_function_shape='ovo')
	clf.fit(readfile2()[0], readfile2()[1].ravel())
	print(clf.score(readfile3()[0], readfile3()[1]))
	print(metrics.roc_auc_score(readfile3()[1], clf.predict(readfile3()[0])))

if __name__ == "__main__":
	main()
