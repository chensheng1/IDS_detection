#coding:utf-8:
#!usr/bin/env python
'''
@author:leilf 
@description:app ranking
@create date:2017/06/14
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
	
def get_past_hour_ts(n_hour):
	'''
	获取过去n_hour个小时整点时间戳
	'''
	now_time=time.strftime('%Y-%m-%d %H',time.localtime(time.time()))
	ts=time.mktime(time.strptime(now_time,'%Y-%m-%d %H'))-3600*n_hour
	return ts
	
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
	df_drop=df.iloc[:,[0,2,12]]
	usagent_content=df_drop.dropna(axis=0)#过滤内容为空的
	result=usagent_content.groupby([0,2,12])#聚合[ip,usagent]
	
	app_use=[]#user,name,duration
	for app in result.groups:
		time=app[0]
		ip=app[1]
		app_name=app[2]
		if len(app_name)>5:		
			if  app_name.startswith('Mozilla') or  app_name.startswith('Dalvik')or app_name.startswith('Safari') or app_name.startswith('Opera'):
				continue
				
			else:
				
				if app_name.startswith('Youku') or app_name.startswith('youku-tudou') :
					app_use.append([time,ip,'Youku'])
					
				elif app_name.startswith('ZhuiShuShenQi'):
					app_use.append([time,ip,'ZhuiShuShenQi'])
					
				elif app_name.startswith('Zeus'):
					app_use.append([time,ip,'Zeus'])
				
				elif app_name.startswith('YYMobile'):
					app_use.append([time,ip,'YYMobile'])
					
				elif app_name.startswith('ynote'):#有道笔记
					app_use.append([time,ip,'ynote'])
				
				elif app_name.startswith('youdao_dict'):#有道词典
					app_use.append([time,ip,'youdao_dict'])
				
				elif app_name.startswith('xiami'):#虾米
					app_use.append([time,ip,'xiami']) 
															
				elif app_name.startswith('Xunlei'):
					app_use.append([time,ip,'xunlei'])
				
				elif app_name.startswith('Xfplay'):
					app_use.append([time,ip,'Xfplay'])
					
				elif app_name.startswith('wpsoffice'):
					app_use.append([time,ip,'wpsoffice'])
				
				elif app_name.startswith('wifikey') or app_name.startswith('wfbl'):
					app_use.append([time,ip,'wifikey'])	
				
				elif app_name.startswith('WeRead') :
					app_use.append([time,ip,'WeRead'])
					
				elif app_name.startswith('Weibo')or app_name.startswith('SinaNews'):
					app_use.append([time,ip,'Weibo'])

				elif app_name.startswith('WeChat') or app_name.startswith('MicroMessenger'):
					app_use.append([time,ip,'WeChat'])
				
				elif app_name.startswith('Valve') :
					app_use.append([time,ip,'Valve'])
					
				elif app_name.startswith('UC/'):
					app_use.append([time,ip,'UC'])#
				
				elif app_name.startswith('Tga') or app_name.startswith('TGA'):
					app_use.append([time,ip,'Tga'])
				
				elif app_name.startswith('ttplayer'):
					app_use.append([time,ip,'ttplayer'])
					
				elif app_name.startswith('ting'):#百度音乐
					app_use.append([time,ip,'ting'])
					
				elif app_name.startswith('TBClient') or app_name.startswith('Taobao') :
					app_use.append([time,ip,'Taobao'])
				
				elif app_name.startswith('SOHUVideoHD')or app_name.startswith('sohuiPadVideo'):
					app_use.append([time,ip,'SOHUVideo'])
					
				elif app_name.startswith('ShanbayWords') :
					app_use.append([time,ip,'ShanbayWords'])
					
				elif app_name.startswith('sogou_ime'):
					app_use.append([time,ip,'sogou'])
				
				elif app_name.startswith('smoba') :
					app_use.append([time,ip,'wzry'])
				
				elif app_name.startswith('SohuNews'):
					app_use.append([time,ip,'SohuNews'])
					
				elif app_name.startswith('Qzone') or app_name.startswith('qzone')or app_name.startswith('android-qzone'):
					app_use.append([time,ip,'Qzone'])

				elif app_name.startswith('QYPlayer') or app_name.startswith('QIYIVideo') or app_name.startswith('iQiYiPhoneVideo'):#爱奇艺iQiYiPhoneVideo
					app_use.append([time,ip,'QYPlayer'])
					
				elif app_name.startswith('qqlive'):
					app_use.append([time,ip,'qqlive'])
					
				elif app_name.startswith('QQGame'):
					app_use.append([time,ip,'QQGame'])
				
				elif app_name.startswith('QQMusic')or app_name.startswith('ANDROIDQQMUSIC'):
					app_use.append([time,ip,'QQMusic'])
					
				elif app_name.startswith('qqpy')or app_name.startswith('qqppim'):
					app_use.append([time,ip,'qqpy'])
					
				elif app_name.startswith('PandaReader'):
					app_use.append([time,ip,'PandaReader'])
				
				elif app_name.startswith('PPStream'):
					app_use.append([time,ip,'PPStream'])
					
				elif app_name.startswith('PandaTV') or app_name.startswith('pandatv') or app_name.startswith('pandaTV'):
					app_use.append([time,ip,'PandaTV'])
					
				elif app_name.startswith('News'):
					app_use.append([time,ip,'News'])
				
				elif app_name.startswith('NeteaseMusic'):
					app_use.append([time,ip,'NeteaseMusic'])
					
				elif app_name.startswith('netdisk'):
					if app_name.find('PC-Windows'):
						continue
					else:
						app_use.append([time,ip,'netdisk'])
					
				elif app_name.startswith('MQQBrowser')or app_name.startswith('QQBrowser'):
					app_use.append([time,ip,'MQQBrowser'])
					
				elif app_name.startswith('MiuiMusic'):
					app_use.append([time,ip,'MiuiMusic'])
					
				elif app_name.startswith('Miaopai'):
					app_use.append([time,ip,'Miaopai'])
					
				elif app_name.startswith('MobileMap') :
					app_use.append([time,ip,'MobileMap'])
					
				elif app_name.startswith('Mogujie'):
					app_use.append([time,ip,'Mogujie'])
				
				elif app_name.startswith('MONO') :
					app_use.append([time,ip,'MONO'])
					
				elif app_name.startswith('MomoChat') :
					app_use.append([time,ip,'Momo'])
				
				elif app_name.startswith('MGTV'):
					app_use.append([time,ip,'MGTV'])	
					
				elif app_name.startswith('live') :
					app_use.append([time,ip,'live'])
				
				elif app_name.startswith('LOFTER'):
					app_use.append([time,ip,'LOFTER'])
				
				elif app_name.startswith('Letv'):
					app_use.append([time,ip,'Letv'])
					
				elif app_name.startswith('kugou/'):
					app_use.append([time,ip,'kugou'])
				
				elif app_name.startswith('kwai'):
					app_use.append([time,ip,'kwai'])
					
				elif app_name.startswith('hearthstone'):
					app_use.append([time,ip,'hearthstone'])#HCDNLivenet6/6.0.3.64
					
				elif app_name.startswith('Jdipad') or app_name.startswith('jdapp')or app_name.startswith('JD'):
					app_use.append([time,ip,'JD'])
				
				elif app_name.startswith('Jike'):
					app_use.append([time,ip,'Jike'])
					
				elif app_name.startswith('iFLYCloud'):
					app_use.append([time,ip,'iFLYCloud'])
					
				elif app_name.startswith('IPadQQ') or app_name.startswith('QQ/')or app_name.startswith('QQClient') or app_name.startswith('QQ D'):
					app_use.append([time,ip,'QQ'])
					
				elif app_name.startswith('HiSpace') or app_name.startswith('hispace'):
					app_use.append([time,ip,'HiSpace'])
					
				elif app_name.startswith('HunanTV')or app_name.startswith('MGTV'):
					app_use.append([time,ip,'MGTV'])
				
				elif app_name.startswith('Flipboard'):
					app_use.append([time,ip,'Flipboard'])
				
				elif app_name.startswith('Fanli'):
					app_use.append([time,ip,'Fanli'])
					
				elif app_name.startswith('Funshion'):
					app_use.append([time,ip,'Funshion'])
					
				elif app_name.startswith('fenghuangdiantai') :
					app_use.append([time,ip,'fenghuangdiantai'])
					
				elif app_name.startswith('esbook'):
					app_use.append([time,ip,'esbook'])
					
				elif app_name.startswith('DYZB'):
					app_use.append([time,ip,'DYZB'])

				elif app_name.startswith('ComicReader'):
					app_use.append([time,ip,'ComicReader'])#
				
				elif app_name.startswith('Changba'):
					app_use.append([time,ip,'Changba'])
				
				elif app_name.startswith('Cupid'):
					app_use.append([time,ip,'Cupid'])
					
				elif app_name.startswith('BaiduHD'):
					app_use.append([time,ip,'BaiduHD'])
				
				elif app_name.startswith('bdtb') :#TBClient
					app_use.append([time,ip,'bdtb'])
					
				elif app_name.startswith('bukaios'):
					app_use.append([time,ip,'bukaios'])
				
				elif app_name.startswith('Blued'):
					app_use.append([time,ip,'Blued'])
					
				elif app_name.startswith('BtcTrade'):
					app_use.append([time,ip,'BtcTrade'])
				
				elif app_name.startswith('Android QQMail '):
					app_use.append([time,ip,'QQMail '])
				
				elif app_name.startswith('AMAP'):
					app_use.append([time,ip,'gd MAP'])
					
				elif app_name.startswith('AiMeiTuan'):
					app_use.append([time,ip,'AiMeiTuan'])
				
					
				elif app_name.startswith('alipay')or app_name.startswith('Alipay') : 
					app_use.append([time,ip,'alipay'])

				elif app_name.startswith('AppStore'):
					app_use.append([time,ip,'AppStore'])
				
				elif app_name.startswith('360freewifi'):
					app_use.append([time,ip,'360freewifi'])
	
				else:                                      
					if len(app_name)>50 and app_name.find('qqnews')>0:
						app_use.append([time,ip,'QQ'])
						
					elif len(app_name)>25 and app_name.find('_weibo_')>0:
						app_use.append([time,ip,'Weibo'])
						
					elif len(app_name)>58 and app_name.find('ANDROID_TB')>0:
						app_use.append([time,ip,'Taobao'])
					
					
		else:
			continue			
	#print df_app_use
	return app_use

def get_database_df(date_time):
	'''
	获取date_time当天数据库中的数据
	返回dataframe
	'''
	sql="select user,app_name,duration from my_app where date='%s'"%(date_time)
	sql_result=db_base.execute_result(sql)
	np_list=[]
	for i in sql_result:
		np_list.append([i[0],i[1],i[2]])
	df_result=pd.DataFrame(np_list,columns=['user', 'app_name','duration'])
	return df_result
	
def to_database():
	data=get_usagent_content()
	if len(data)==0:
		return 
	sql1="insert into app(date,user,app_name) values"
	for idata in data:
		time_data=idata[0]
		_user=idata[1]
		_app_name=idata[2]
		sql1=sql1+"('%s','%s','%s')," %(time_data,_user,_app_name)
	sql1=sql1[:-1]
	db_base.execute_no_result(sql1)#批量插入

	

def main():
	to_database()
	#get_usagent_content()
if __name__=='__main__':
	main()
