# -*- coding: utf-8 -*-
'''
 @Time : 2022/5/29 20:25
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
 @File : db_pool.py
 @Project : pycharm
'''

import pymysql
from dbutils.pooled_db import PooledDB

MYSQL_DB_POOL = PooledDB(
    creator=pymysql,
    maxconnections=50,
    mincached=2,
    maxcached=10,
    blocking=True,
    setsession=[],
    ping=0,
    host='blog.sunliguo.com',
    port=53306,
    user='root',
    password='admin',
    database='crawl',
    charset='utf8'
)


def task():
    conn = MYSQL_DB_POOL.connection()
    cur = conn.cursor()
    cur.execute('show tables;')
    result = cur.fetchall()
    print(result)
    cur.close()
    conn.close()


if __name__ == '__main__':
    task()
