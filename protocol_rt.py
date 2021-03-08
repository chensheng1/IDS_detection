#coding:utf-8
#!usr/bin/env python
'''
@author:lucas 1273085613@qq.com
@description:实时统计应用情况
@create date:2017/09/27
可识别应用有（28种）：
dhcp,dns,http,ssl,teredo,bittorrent,mysql,smtp,ssh,xmpp,ftp,irc,dtls,imap,pop3
sip,modbus,emule,icq,msn,sina,wect,dce_rpc,gnutella1,radius,krb,thunder,rdp
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


def get_data():
    '''
    获取所需数据
    '''
    file_path_all=file_path.current_log_file_path+'conn.log'
    #file_path_all='conn.log'
    if not os.path.exists(file_path_all):
        return 0
    else:
        all_data=read_file.pandas_normal(file_path_all)
        value=all_data.iloc[:-1,[7,17,19]]#service,orig_bytes,resp_bytes
        useful_data=value.dropna(how='any') # 去掉包含缺失值的行
        useful_data.rename(columns={7:'service'},inplace = True)
        useful_data.rename(columns={17:'orig_bytes'},inplace = True)
        useful_data.rename(columns={19:'resp_bytes'},inplace = True)
        data=useful_data.groupby('service').sum()
        return data
def clear_database():
    sql0="delete from protocol_rt"
    db_base.execute_no_result(sql0)
def write_databae():
    data=get_data()
    if  not isinstance(data,pd.DataFrame):
        return 
    for i in list(data.index):
        if str(i).find(',')==-1 and str(i).find('-')==-1:
            sql1="insert into protocol_rt(protocol,incomingTraff,outgoingTraff) values('%s',%d,%d)" %(i,data.loc[i,'resp_bytes'],data.loc[i,'orig_bytes'])
            db_base.execute_no_result(sql1)
    
def main():
    #print get_data()
    clear_database()
    write_databae()

if __name__=='__main__':
    main()
