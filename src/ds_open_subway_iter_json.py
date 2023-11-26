#!/usr/bin/env python
# coding: utf-8
import os
import requests
import urllib
import mylib # NO! src.mylib

def doIt():
    keyPath=os.path.join(os.getcwd(), 'src', 'key.properties')
    key=mylib.getKey(keyPath)
    # (1) make params with resource IDs
    KEY=str(key['dataseoul'])
    TYPE='json'
    #OLD: SERVICE='SearchSTNBySubwayLineService'
    SERVICE='SearchSTNBySubwayLineInfo'
    LINE_NUM=str(2)
    START_INDEX=str(1)
    END_INDEX=str(10)
    startIndex=1
    endIndex=10
    list_total_count=0    # set later
    while True:
        START_INDEX=str(startIndex)
        END_INDEX=str(endIndex)
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
        if(startIndex==1):
            list_total_count=stations['SearchSTNBySubwayLineInfo']['list_total_count']
            print("- Total Count: ", list_total_count)
        if(stations["SearchSTNBySubwayLineInfo"]["RESULT"]["CODE"]=="INFO-000"):
            for e in stations['SearchSTNBySubwayLineInfo']['row']:
                print (u"{0:3d}\t{1:4.0f}\t{2:15s}\t{3:3s}\t{4:2s}".format(
                    startIndex, e['STATION_CD'], e['STATION_NM'], e['FR_CODE'], e['LINE_NUM']
                ))
            startIndex+=10
            endIndex+=10
        else:
            print("Error: "+str(startIndex))
        if(endIndex > list_total_count):
            print("----- Ending endIndex=",endIndex)
            break  # exit from the while loop

if __name__ == "__main__":
    doIt()
    
