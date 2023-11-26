#!/usr/bin/env python
# coding: utf-8
import os
import requests
import urllib
import mylib # NO! src.mylib -> 상대적 디렉터리이기 때문. src 밑에 저장하기 때문

def doIt():
    keyPath=os.path.join(os.getcwd(), 'src', 'key.properties')
    key=mylib.getKey(keyPath)
    
    # (1) make params with resource IDs
    KEY=str(key['dataseoul'])
    TYPE='json'
    SERVICE='SearchSTNBySubwayLineInfo'
    LINE_NUM=str(2)
    START_INDEX=str(1)
    END_INDEX=str(10)
    
    #params=os.path.join(KEY,TYPE,SERVICE,START_INDEX,END_INDEX,LINE_NUM)
    params="/".join([KEY,TYPE,SERVICE,START_INDEX,END_INDEX,'','',LINE_NUM])
    
    # (2) make a full url
    _url='http://openAPI.seoul.go.kr:8088' #NOTE slash: do not use 'http://openAPI.seoul.go.kr:8088/'
    #url=urllib.parse.urljoin(_url,params)
    url="/".join([_url,params])
    #print(url)
    
    # (3) get data
    r=requests.get(url)
    #print(r)
    stations=r.json()
    #print(stations)
    for e in stations['SearchSTNBySubwayLineInfo']['row']:
        print (u"{0:4.0f}\t{1:15s}\t{2:3s}\t{3:2s}".format(e['STATION_CD'], e['STATION_NM'], e['FR_CODE'], e['LINE_NUM']))

if __name__ == "__main__":
    doIt()
