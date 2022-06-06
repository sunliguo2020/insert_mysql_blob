# -*- coding: utf-8 -*-
"""
@author: sunliguo
@contact: QQ376440229
@Created on: 2022/4/8 19:32
"""
import pymysql


def get_file_from_mysql(file_name, table):
    """

    :param file_name: 要查询的文件名
    :param table:   要查询的数据表
    :return:
    """
    conn = pymysql.Connect(host='127.0.0.1',
                           port=3306,
                           user='root',
                           passwd='',
                           db='crawl')
    cur = conn.cursor()
    sql = f'select `file_name`,`blob` from `{table}` where `file_name` like "{file_name}.txt" limit 1;'
    print(sql)
    cur.execute(sql)
    result = cur.fetchone()
    print(cur.description)
    while result:
        yield result
        result = cur.fetchone()


if __name__ == '__main__':
    with open('phone.txt', 'r') as fp:
        for phone in fp:
            phone = phone.replace('\n', '')
            for file_name, blob in get_file_from_mysql(phone, 'phone'):
                # with open(file_name, 'wb') as f:
                #     f.write(blob)
                break
