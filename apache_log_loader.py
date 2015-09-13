#!/usr/local/bin/python
# -*- coding:utf-8 -*-
import apache_log_parser
import pymongo
import os

def main():

    # パーサーを作成
    # 指定するのは、httpd.confに記載しているLogFormatの書式
    parser = apache_log_parser.make_parser('%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"')

    # MongoDB接続 
    conn = pymongo.MongoClient('localhost', 27017)
    db = conn.apache
    collection = db.log

    # access_logで開始するアクセスログのリストを取得
    access_log_list = get_access_log_list()
    for file in access_log_list:
        for line in open('apache_log/' + file):
            # parse
            log_data = parser(line)

            # load to db
            load_to_db(collection, log_data)

def load_to_db(collection, log_data):
    collection.save(log_data)
    
def get_access_log_list():
    access_log_list = []
    files = os.listdir('apache_log/')
    for file in files:
        if file.startswith('access_log'):
            access_log_list.append(file)

    return access_log_list

if __name__ == '__main__':
    main()

