#!/usr/local/bin/python
# -*- coding:utf-8 -*-
import apache_log_parser
import pymongo
import json
from geoip import geolite2
from bson import json_util
from geoip import geolite2

def application(environ, start_response):
    # MongoDB接続 
    conn = pymongo.MongoClient('localhost', 27017)
    db = conn.apache
    collection = db.log
    list = []
    for data in collection.find():
        match = geolite2.lookup(data['remote_host'])
        if match != None:
            data.update({"country":match.country})
            data.update({"location":match.location})
            list.append(data)
    start_response('200 OK', [('Content-type', 'application/json')])
    return json.dumps(list, default=json_util.default)
