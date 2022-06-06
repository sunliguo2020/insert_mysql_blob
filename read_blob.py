# -*- coding: utf-8 -*-
"""
@author: sunliguo
@contact: QQ376440229
@Created on: 2022/3/18 7:32
"""
import pymysql


def write_blob(blob_file, data):
    with open(blob_file, 'wb') as fp:
        fp.write(data)


def read_blob(md5sum='', database='crawl', table=''):
    """
    从crawl中查询md5值
    """

    try:
        conn = pymysql.Connect(host='192.168.1.207',
                               port=3306,
                               user='root',
                               password='admin',
                               db=database)
        cur = conn.cursor()
    except Exception as e:
        print("连接数据库失败，", e)
        return -1

    sql = f'select `file_name` ,`blob` from {table} where md5sum = "{md5sum}";'
    # print(sql)
    cur.execute(sql)
    result = cur.fetchone()
    if result:
        file_name = result[0]
        blob_file = result[1]
        write_blob(file_name, blob_file)
        #print(blob_file.decode('gb2312'))
    else:
        print("查询失败！")
    cur.close()
    conn.close()


if __name__ == '__main__':
    data = read_blob(md5sum='eabfb9182788b374596af654589ea31e', table='DaglPerson')
    print(data)
