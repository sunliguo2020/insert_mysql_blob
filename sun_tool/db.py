# -*- coding: utf-8 -*-
"""
 @Time : 2022/5/29 20:45
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
 @File : db.py

"""
import logging
import pymysql

from dbutils.pooled_db import PooledDB

logger = logging.getLogger('my_project')


class DBHelper(object):
    """
    Mysql 数据库操作助手
    """

    def __init__(self,
                 host=None,
                 port=3306,
                 user=None,
                 password=None,
                 database='crawl'):
        """
        初始化数据库连接池
        @param host:数据库主机地址
        @param user:数据库用户名
        @param password:数据库密码
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

    def get_connection(self):
        """
        获取数据库连接和游标
        @return: (connection,cursor) 元组
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

    def exec(self, sql, params=None):
        """
        执行SQL语句（不返回结果）
        @param params: SQL参数，元组或列表
        @param sql:SQL语句
        """
        try:
            conn, cur = self.get_connection()
            cur.execute(sql, params)
            conn.commit()
        except pymysql.Error as e:
            logging.error(f"SQL执行失败：{e}")
        finally:
            cur.close()
            conn.close()

    def fetch_one(self, sql, **kwargs):
        """
        执行SQL语句并返回一行结果
        :param sql:SQL语句
        :param kwargs: SQL 参数，字典格式，但内部会转换为元组
        :return:一行结果或None
        """
        params = tuple(kwargs.values()) if kwargs else None

        # print(f"kwargs:{kwargs},params:{params}")

        try:
            conn, cur = self.get_connection()
            cur.execute(sql, params)
            result = cur.fetchone()
        except pymysql.Error as e:
            logging.error(f"SQL 查询失败:{e}")
            result = None
        finally:
            self.close_conn_cursor(cur, conn)

        return result

    def fetch_all(self, sql, **kwargs):
        """
        执行SQL语句并返回所有结果
        @param sql:SQL语句
        @param kwargs:SQL参数 ，字典格式，但内部会转换为元组
        @return: 结果列表
        """
        params = tuple(kwargs.values()) if kwargs else None
        try:
            conn, cur = self.get_connection()
            cur.execute(sql, params)
            result = cur.fetchall()
        except pymysql.Error as e:
            logging.error(f"SQL 查询失败: {e}")
            result = []
        finally:
            self.close_conn_cursor(cur, conn)
        return result
