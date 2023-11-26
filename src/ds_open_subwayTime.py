# coding: utf-8
import os
import requests
import json
from pymongo import MongoClient
import mylib

# JSON 파일 저장하기
def saveJson(_fname,_data):
    import io
    with io.open(_fname, 'a', encoding='utf8') as json_file:
        #_j=json.dumps(_data, json_file, ensure_ascii=False, encoding='utf8') # python2
        _j=json.dump(_data, json_file, ensure_ascii=False)
        json_file.write(str(_j)+"\n")

def readJson(_fname):
    for line in open(_fname, 'r').readlines():
        _j=json.loads(line)
        #print _j['id'],_j['text']
        print (_j['id'])

# 데이터베이스에 저장하기        
def saveDB(_table, _data):
    _table.insert_one(_data)

# Update Required: tweet -> ds_open_subwayPassengersDb
def readDB(_table):
    for tweet in _table.find():
        print (tweet['id'], tweet['text'])

def saveFile(_fname,_data):
    fp=open(_fname,'a')
    fp.write(_data+"\n")
    fp.close()

def doIt():
    # 1. Key.properties 가져오기
    keyPath=os.path.join(os.getcwd(), 'src', 'key.properties')
    key=mylib.getKey(keyPath)
    
    # 2. mongodb connect
    mongo_pwd = key['mongo']
    uriCloud = f'mongodb+srv://sjh9708:{mongo_pwd}@mycluster.tfiobq6.mongodb.net/?retryWrites=true&w=majority'
    Client = MongoClient(uriCloud)
    _db=Client['ds_open_subwayPassengersDb'] #db created by mongo. You do not have to create this.
    _table=_db['db_open_subwayTable'] #collection
    
    # 3. 저장할 JSON 파일명
    _jfname='src/ds_open_subwayTime.json'

    # 4. OpenAPI dataseoul에서 수집
    _key=key['dataseoul'] #KEY='73725.....'
    _url='http://openAPI.seoul.go.kr:8088'
    _type='json'
    _service='CardSubwayTime'
    _start_index=1
    _end_index=5
    _use_mon='202106'
    _maxIter=2   #20
    _iter=0
    while _iter<_maxIter:
        # 5. API URL 작성
        _api="/".join([_url,_key,_type,_service,str(_start_index),str(_end_index),_use_mon])

        # 6. API 요청
        r=requests.get(_api)
        
        # 7. JSON Parsing
        _json=r.json()
        print (_json)
        
        # 8. JSON 파일 / MongoDB에 JSON 저장
        saveJson(_jfname,_json)
        saveDB(_table, _json)
        _start_index+=5
        _end_index+=5
        _iter+=1

if __name__ == "__main__":
    doIt()
