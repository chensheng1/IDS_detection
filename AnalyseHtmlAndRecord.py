#coding:utf-8
from pyquery import PyQuery as pq
from jieba import analyse
import sys
import os
import MySQLdb
import datetime
import traceback

#htmlPath = path_config.get_html_path()
#logPath = path_config.get_http_path()

class readHtml:
    '从文件路径获取文件并提取关键字'
    def __init__(self,filePath):
        d = pq(filename=filePath)
        self.title=d('title').text()
    def textTank(self):
        textrank = analyse.textrank
        keywords = textrank(self.title)
        return keywords
    def tfidf(self):
        tfidf = analyse.extract_tags
        keywords = tfidf(self.title)
        return keywords

'获取html文件夹下文件路径'
def htmllistdir(path, list_name):  
    for file in os.listdir(path):  
        file_path = os.path.join(path, file)  
        if os.path.isdir(file_path):  
            #listdir(file_path, list_name)
            a=1
        elif os.path.splitext(file_path)[1] == '.html':  
            list_name.append(file_path)

'获取log文件夹下文件路径'
def loglistdir(path, list_name):  
    for file in os.listdir(path):  
        file_path = os.path.join(path, file)  
        if os.path.isdir(file_path):  
            #listdir(file_path, list_name)
            a=1
        elif os.path.splitext(file_path)[1] == '.log':
            list_name.append(file_path)

'获取HTML文件的哈希名称'
def getFileName(filePath):
    return filePath.split("-")[-1].split(".")[0]

'获取HTML文件对应的url、time、orig_ip'
def getUrl(logPath,htmlName):
    logList = []
    
    url="-"
    time="-"
    orig_ip="-"
    loglistdir(logPath,logList)
    for log in logList:
        all_the_text = open(log).read()
        
        l = all_the_text.split("\t")
        for s in l:
            if s==getFileName(htmlName):
                urlIndex = l.index(s)-18
                url = l[urlIndex] + l[urlIndex+1]
                '获取log文件中的time'
                timeIndex = l.index(s)-26
                time = time.strftime("%Y--%m--%d %H:%M:%S",time.localtime(int(float(l[timeIndex]))))
                '获取log文件中的orig_ip'
                orig_ip = l[timeIndex+2]

    dict_result = {'url':url,'time':time,'orig_ip':orig_ip}
    return dict_result
	
def HTMLanalysis():
	htmlPath ='/usr/local/data/extract_files/'
	logPath ='/usr/local/data/'
	conn=MySQLdb.connect(host="127.0.0.1",port=3306,user="root",passwd="*Hubu1411",db="hubutraff",charset="utf8")
	cursor = conn.cursor()
	sqlInsert = "INSERT INTO `hubutraff`.`analyserecord` (`time`, `orig_ip`,`keyword`,`url`) VALUES ('%s','%s','%s','%s')"
	try:
		htmlList = []
		htmllistdir(htmlPath,htmlList)
		for html in htmlList:
			dict_result = getUrl(logPath,html)
			rh = readHtml(html)
			for key in rh.tfidf():
				cursor.execute(sqlInsert % (dict_result['time'],dict_result['orig_ip'],key,dict_result['url']))
				
		
		conn.commit()
	except:
		traceback.print_exc()
		conn.rollback()
	finally:
		cursor.close()
		conn.close()

if __name__=='__main__':
	HTMLanalysis()
'''
if __name__=='__main__':
    conn=MySQLdb.connect(host="127.0.0.1",port=3306,user="root",passwd="*Hubu1411",db="hubutraff",charset="utf8")
    cursor = conn.cursor()
    sqlInsert = "INSERT INTO `hubutraff`.`analyserecord` (`RecordId`, `KeyWord`,`KeyType`,`url`,`Time`) VALUES (uuid(),'%s','%s','%s',str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'))"
    try:
        htmlList = []
        htmllistdir(htmlPath,htmlList)
        for html in htmlList:
            url = getUrl(logPath,html)
            rh = readHtml(html)
            for key in rh.tfidf():
                dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(sqlInsert % (key,"-",url,dt))
        
        conn.commit()
    except:
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

'''




















