#!/usr/local/bin/python
# -*- coding:utf-8 -*-
import pymongo
import json
from geoip import geolite2
from bson import json_util
from geoip import geolite2

# MongoDB接続情報
conn = pymongo.MongoClient('localhost', 27017)
db = conn.apache
collection = None

def application(environ, start_response):
    # MongoDB接続 
    collection = db.log

    # 国別アクセス数を格納するdict(alpha-2)
    access_alpha2_dic = {}

    for data in collection.find():
        match = geolite2.lookup(data['remote_host'])
        if match != None:
            country = match.country
            if access_alpha2_dic.has_key(country):
                access_alpha2_dic.update({country:access_alpha2_dic[country] + 1})
            else:
                access_alpha2_dic.update({country:0})
    
    # 国コードをalpha-2からalpha-3に変換
    access_alpha3_dic = convertAlfa3Code(access_alpha2_dic)

    polygon_list = createPolygonDic(access_alpha3_dic)

    polygonGeoJSON = {"type":"FeatureCollection","features":polygon_list}
    #print polygonGeoJSON

    start_response('200 OK', [('Content-type', 'application/json'),('Access-Control-Allow-Origin', "*")])
    return json.dumps(polygonGeoJSON, default=json_util.default)

# 国コードをalpha-2からalpha-3に変更
def convertAlfa3Code(access_alpha2_dic):
    # collection=countryにアクセス
    collection = db.country
   
    # 国別アクセス数を格納するdict(alpha-3)
    access_alpha3_dic = {}
    for key in access_alpha2_dic.keys():
        access_alpha3_dic.update({collection.find_one({'alpha-2': key})['alpha-3']:access_alpha2_dic[key]})

    return access_alpha3_dic

def createPolygonDic(access_alpha3_dic):
    # collection=countries_geoにアクセス
    collection = db.countries_geo

    # アクセス数の合計を算出
    total_access_cnt = 0.0
    for cnt in access_alpha3_dic.values():
        total_access_cnt += cnt

    # polygon情報を格納するlist
    polygon_list = []
    for key in access_alpha3_dic.keys():
        polygon_data = collection.find_one({'id': key})
        if polygon_data != None:
            polygon_data['properties'].update({'rate':access_alpha3_dic[key]/total_access_cnt})
            del(polygon_data['_id'])
            polygon_list.append(polygon_data)
    return polygon_list

if __name__ == '__main__':
    application('hoge', 'hoge')
