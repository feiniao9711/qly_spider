#encoding:utf-8
import gzip 
from io import StringIO
import pandas as pd
import numpy as np
import json
import os
import pymysql

#解压gzip
def gzdecode(data) :
    compressedstream = StringIO(data)
    gziper = gzip.GzipFile(fileobj=compressedstream)  
    data2 = gziper.read()   # 读取解压缩后数据 
    return data2

def generateTimeList(startDate, endDate):
    # print(pd.date_range('11/1/2018','11/9/2018'))
    # print(np.arange('2005-02', '2005-03', dtype='datetime64[D]'))
    return np.arange(startDate, endDate, dtype='datetime64[D]')

def generateInt():
    print(np.arange(0, 24)) 

def generateQLYUrl(startDate, endDate):
    urlPrefix='http://113.57.190.228:8001/Web/Report/GetRiverData?date='
    # 2020-06-11+08%3A00'
    dateArray = np.arange(startDate, endDate, dtype='datetime64[D]')
    urls = []
    for day in dateArray:
        for i in range(24):
            # ts = pd.to_datetime(str(day))
            # d = ts.strftime('%Y-%m-%d')
            timeStr = str(day) + '+' + str(i).zfill(2) + '%3A00'
            url = urlPrefix + timeStr
            urls.append(url)
    # print(urls)
    np.savetxt("dates.txt", urls, fmt='%s', newline='\n')

def convertQLYDataToCSV(theYear):
    # filePath="D:\\qly_spider\\result.txt"
    # file = open(filePath, encoding="UTF-8")
    # text=file.read()
    # file.close()
    column = ['日期', '时间', '河名', '站号', '站名', '水位', '水势', '比昨日</br>+涨-落', '流量', '设防</br>水位', '警戒</br>水位', '最高</br>水位']
    db = pymysql.connect("localhost","root","123456","weather", charset='utf8')
    cursor = db.cursor()
    dataList = []
    try:
        # 执行SQL语句
        cursor.execute("SELECT url, `result`, theDay, theTime FROM weather.qly2020 where theYear = %s order by theYear , theDay , theTime asc", theYear)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            # url = row[0]
            # theDay = url[57:10]
            # theTime = url[68:].replace('%3A', ':')
            result = row[1]
            theDay = row[2]
            theTime = row[3]
            text_json = json.loads(result)
            waters = text_json['rows']
            for water in waters:
                dataLine = []
                dataLine.append(theDay)
                dataLine.append(theTime)
                dataLine.append(water['RVNM']) #河名
                dataLine.append(water['STCD'])  #站号
                dataLine.append(water['STNM'])  #站名
                dataLine.append(water['Z'])  #水位
                dataLine.append(water['WPTN']) #水势
                dataLine.append(water['YZ']) #涨落
                dataLine.append(water['Q']) #流量
                dataLine.append(water['FRZ']) #设防水位
                dataLine.append(water['WRZ']) #警戒水位
                # dataLine.append(water['GRZ']) 
                dataLine.append(water['MAXZ']) #最高水位
                dataList.append(dataLine)
                # second line
                dataLine1 = []
                dataLine1.append(theDay)
                dataLine1.append(theTime)
                dataLine1.append(water['RVNM1'])
                dataLine1.append(water['STCD1'])
                dataLine1.append(water['STNM1'])
                dataLine1.append(water['Z1'])
                dataLine1.append(water['WPTN1'])
                dataLine1.append(water['YZ1'])
                dataLine1.append(water['Q1'])
                dataLine1.append(water['FRZ1'])
                dataLine1.append(water['WRZ1'])
                # dataLine1.append(water['GRZ1'])
                dataLine1.append(water['MAXZ1'])
                dataList.append(dataLine1)
    except Exception as e:
        print(e.args)
        print(str(e))
        print(repr(e))

    # 关闭数据库连接
    db.close()

    result=pd.DataFrame(columns=column,data=dataList)
    filePath='d:/qly/qly_' + theYear + '.csv'
    # os.remove(filePath)
    result.to_csv(filePath)
    


if __name__ == '__main__':
#    generateQLYUrl('2020-01', '2020-08')
    convertQLYDataToCSV('2020')