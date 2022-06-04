# -*- coding: utf-8 -*-
'''
 @Time : 2022/5/29 20:45
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
 @File : db.py
 @Project : pycharm
'''
import pymysql
from dbutils.pooled_db import PooledDB


class DBHelper(object):
    def __init__(self):
        self.pool = PooledDB(
            creator=pymysql,
            maxconnections=50,
            mincached=2,
            maxcached=10,
            blocking=True,
            setsession=[],
            ping=0,
            host='127.0.0.1',
            port=3306,
            user='root',
            password='admin',
            database='crawl',
            charset='utf8'
        )

    def get_conn_cursor(self):
        conn = self.pool.connection()
        cursor = conn.cursor()
        return conn, cursor

    def close_conn_cursor(self, *args):
        for item in args:
            item.close()

    def exec(self, sql, **kwargs):
        conn, cur = self.get_conn_cursor()
        cur.execute(sql, kwargs)
        conn.commit()

    def fetch_one(self, sql, kwargs):
        """
        
        :param sql:
        :param kwargs:
        :return:
        """
        print("fetch one ")
        print(sql)
        print(kwargs)
        conn, cur = self.get_conn_cursor()
        cur.execute(sql, kwargs)
        # cur.execute(sql)
        result = cur.fetchone()
        self.close_conn_cursor(cur, conn)
        return result

    def fetch_all(self, sql, **kwargs):
        print("fetch all")
        conn, cur = self.get_conn_cursor()
        cur.execute(sql, kwargs)
        result = cur.fetchall()
        self.close_conn_cursor(cur, conn)
        return result


db = DBHelper()
