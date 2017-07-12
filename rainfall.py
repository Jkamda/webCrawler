#-*- coding: utf-8 -*-
import pymysql
import urllib
import sys
import os
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date


reload(sys)
sys.setdefaultencoding('utf-8')

""" 
python rainfall.py 시작날짜 종료날짜
python rainfall.py 20170501 20170531
"""

STARTDATE=datetime.strptime(sys.argv[1], '%Y%m%d').date()
ENDDATE=datetime.strptime(sys.argv[2], '%Y%m%d').date()

"""
원하는 지역 추가

"""
locations=[]
locations.append('용인')
#locations.append('수원')

"""
DB 정보 예제
DB Name : rainfall
User : rainfall
Password : rainfall
TABLE NAME : RAINFALL_DATA
TABLE COLUMN : SEQ, RAINFALL_DATE, AWS_NUM, AWS_NAME, AWS_LOCATION, RAINFALL, TEMPERATUER
"""
def connectInfo():
    HOST='192.168.56.210'
    USER='rainfall'
    PASSWORD='rainfall'
    DB='rainfall'
    CHARSET='utf8'
    PORT=3306
    connect = pymysql.connect(host=HOST, 
                       user=USER, 
                       password=PASSWORD, 
                       db=DB, 
                       charset=CHARSET, 
                       port=PORT)    
    return connect


"""
INSERT DATA 
"""
def insertData(conn,RAINFALL_DATE,AWS_NUM,AWS_NAME,AWS_LOCATION,RAINFALL,TEMPERATUER):
    curs = conn.cursor()
    sql="INSERT INTO RAINFALL_DATA (RAINFALL_DATE, AWS_NUM, AWS_NAME, AWS_LOCATION, RAINFALL, TEMPERATUER) VALUES (%s, %s, %s, %s, %s, %s)"
    curs.execute(sql, (RAINFALL_DATE,AWS_NUM,AWS_NAME,AWS_LOCATION,RAINFALL,TEMPERATUER))
    conn.commit()
    conn.close()


"""
데이터 파서
(AWS 고유 번호, 고유 이름, 일강수, 온도, 위치)
"""
def cycleDate(STARTDATE, ENDDATE) :
    while(STARTDATE) :
        time.sleep(2)
        urlTargetDate=str(STARTDATE).replace('-', '')              
        AWS_PAGE = urllib.urlopen("http://www.kma.go.kr/cgi-bin/aws/nph-aws_txt_min?" + urlTargetDate + "&720").read().decode('euc-kr' ,'replace').encode('utf-8','replace')
        soup = BeautifulSoup(AWS_PAGE, 'html.parser')
        for row in soup.find_all('tr', attrs={'class':'text'}):
            try :
                awsLocation= str(row.find_all('td')[18].string)
                for location in locations :
                    if awsLocation.count(str(location)):
                        RAINFALL_DATE=STARTDATE
                        AWS_NUM=str(row.find_all('td')[0].string) #AWS 지점 : 고유 번호
                        AWS_NAME=str(row.find_all('td')[1].string) #AWS 지점 : 고유 이름
                        RAINFALL=str(row.find_all('td')[8].string) #일강수
                        TEMPERATUER=str(row.find_all('td')[9].string) #온도
                        AWS_LOCATION=str(row.find_all('td')[18].string) #위치    
                        conn = connectInfo()
                        insertData(conn,RAINFALL_DATE,AWS_NUM,AWS_NAME,AWS_LOCATION,RAINFALL,TEMPERATUER)                                
            except :
                pass                
        STARTDATE += timedelta(days=1)        
        if ENDDATE == STARTDATE :
            print "완료"
            break


cycleDate(STARTDATE, ENDDATE)

