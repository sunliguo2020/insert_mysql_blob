# -*- coding: utf-8 -*-
"""
 @Time : 2022/5/29 20:45
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
 @File : db.py

"""
import pymysql
from dbutils.pooled_db import PooledDB


class DBHelper(object):
    """
    Mysql 小助手
    """

    def __init__(self,
                 host=None,
                 port=3306,
                 user=None,
                 password=None,
                 database='crawl'):
        """

        @param host:
        @param user:
        @param password:
        """
        self.pool = PooledDB(
            creator=pymysql,
            maxconnections=50,
            mincached=2,
            maxcached=10,
            blocking=True,
            setsession=[],
            ping=0,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset='utf8'
        )

    def get_conn_cursor(self):
        """

        @return:
        """
        conn = self.pool.connection()
        cursor = conn.cursor()
        return conn, cursor

    def close_conn_cursor(self, *args):
        """

        @param args:
        """
        for item in args:
            item.close()

    def exec(self, sql, args=None):
        """

        @param sql:
        @param args:
        """
        conn, cur = self.get_conn_cursor()
        cur.execute(sql, args)
        conn.commit()

    def fetch_one(self, sql, **kwargs):
        """
        :param sql:
        :param kwargs:
        :return:
        """
        result = None
        try:
            conn, cur = self.get_conn_cursor()
            cur.execute(sql, kwargs)
            result = cur.fetchone()

        finally:
            self.close_conn_cursor(cur, conn)

        return result

    def fetch_all(self, sql, **kwargs):
        """

        @param sql:
        @param kwargs:
        @return:
        """
        print("fetch all")
        conn, cur = self.get_conn_cursor()
        cur.execute(sql, kwargs)
        result = cur.fetchall()
        self.close_conn_cursor(cur, conn)
        return result
