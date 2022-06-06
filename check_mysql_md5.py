# -*- coding: utf-8 -*-
"""
@author: sunliguo
@contact: QQ376440229
@Created on: 2022/3/18 10:19

检查mysql中文件是否保存正确
检查数据库中 MD5值 是否和 保存的文件的md5值是否一样。
2022-04-06:分段取sql
"""

import hashlib

import pymysql


def md5_file(file_content):
    return hashlib.md5(file_content).hexdigest()


def get_sql_md5(table='guhua', database='crawl'):
    """
    # 要检查的数据库和数据表
    """

    conn = pymysql.Connect(host='127.0.0.1',
                           user='root',
                           password='',
                           port=3306,
                           db=database)

    cur = conn.cursor()
    print("初始化游标结束！")
    # 先获取有多少行数据
    sql_count = f'select count(*) from {table}'
    cur.execute(sql_count)
    data_count = cur.fetchone()[0]

    step = 5000
    current_row = 0

    while current_row < data_count:
        sql = f'select `file_name`,`md5sum`,`blob` from {table} limit {current_row}, {step} ;'
        # print(sql)
        cur.execute(sql)
        result = cur.fetchone()
        while result:
            yield result
            result = cur.fetchone()

        current_row = current_row + step


if __name__ == '__main__':

    count = 0
    diff_count = 0

    for i in get_sql_md5(table='sgy_idcard_pic'):
        count += 1
        print("count:", count)
        file_name, md5sum, file_content = i
        sql_md5 = md5_file(file_content)
        if md5sum != sql_md5:
            diff_count += 1
            print(file_name, md5sum, sql_md5, "md5值不同")

    print("总共检查的条数为：", count)
    print("总共不同的条数为：", diff_count)
