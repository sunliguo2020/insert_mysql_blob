# -*- coding: utf-8 -*-
'''
 @Time : 2022/5/29 21:07
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
 @File : db_context.py
 @Project : pycharm
'''
import pymysql
from dbutils.pooled_db import PooledDB

POOL = PooledDB(
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


class Connect(object):
    def __init__(self):
        self.conn = PooledDB.connection()
        self.cur = self.conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()
        self.conn.close()

    def exec(self, sql, **kwargs):
        self.cur.execute(sql, kwargs)
        self.conn.commit()

    def fetch_one(self, sql, **kwargs):
        self.cur.execute(sql, kwargs)
        result = self.cur.fetchone()
        return result

    def fetch_all(self, sql, **kwargs):
        self.cur.execute(sql, kwargs)
        result = self.cur.fetchall()
        return result
