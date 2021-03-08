#!/usr/bin/python
#coding=utf-8

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import tensorflow as tf
import csv
import pandas as pd
import numpy as np
import os
import sys
import urllib.request

def readfile():
	file_path_all='C:\\Users\\qq\\Desktop\\train1.csv'
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
	file_path_all='C:\\Users\\qq\\Desktop\\train2.csv'
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

def main():

	# Specify that all features have real-value data
	feature_columns = [tf.contrib.layers.real_valued_column("", dimension=6)]

	# Build 3 layer DNN with 10, 20, 10 units respectively.
	classifier = tf.contrib.learn.DNNClassifier(feature_columns=feature_columns,
																							hidden_units=[10, 20, 10],
																							n_classes=2,
																							model_dir="/tmp/iris_model")
	# Define the training inputs
	def get_train_inputs():
			x = tf.constant(readfile()[0])
			y = tf.constant(readfile()[1])
			return x, y

	# Fit model.
	classifier.fit(input_fn=get_train_inputs, steps=2000)

	# Define the test inputs
	def get_test_inputs():
			x = tf.constant(readfile1()[0])
			y = tf.constant(readfile1()[1])

			return x, y

	# Evaluate accuracy.
	print(classifier.evaluate(input_fn=get_test_inputs, steps=1))
	accuracy_score = classifier.evaluate(input_fn=get_test_inputs, steps=1)["accuracy"]

	print("nTest Accuracy: {0:f}n".format(accuracy_score))


	'''
	# Classify two new flower samples.
	def new_samples():
			return np.array([[6.4, 3.2, 4.5, 1.5],[5.8, 3.1, 5.0, 1.7]], dtype=np.float32)

	predictions = list(classifier.predict(input_fn=new_samples))

	print("New Samples, Class Predictions:    {}n".format(predictions))
	'''

if __name__ == "__main__":
     main()
