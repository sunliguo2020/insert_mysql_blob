# -*- coding: utf-8 -*-
"""
@author: sunliguo
@contact: QQ376440229
@Created on: 2022/4/8 19:32
在数据库crawl中的某个表中查询是否有某个文件
查询并保存
"""
import pymysql
import os


def get_file_from_mysql(file_name, table):
    """
    从MySQL数据库中查询指定的文件名并返回文件内容和文件名。
    :param file_name: 要查询的文件名 （不包括扩展名.txt)
    :param table:   要查询的数据表名
    :return: 生成器，返回包含文件名和二进制数据的元组
    """
    try:
        # 使用with语句管理数据库连接和游标
        with pymysql.Connect(host='192.168.110.207',
                             port=3306,
                             user='root',
                             passwd='admin',
                             db='crawl') as conn:
            with conn.cursor() as cur:
                # 使用参数化擦寻防止SQL注入
                sql = f'select `file_name`,`blob` ,`md5sum` from `{table}` where `file_name` like %s ;'
                cur.execute(sql, (f"{file_name}",))
                result = cur.fetchone()
                # print(cur.description)
                while result:
                    yield result
                    result = cur.fetchone()
    except pymysql.MySQLError as e:
        print(f"数据库查询出错：{e}")


def write_file_form_mysql(file_name, table):
    """
    从 MySQL 数据库中查询文件内容，并将其保存到本地目录中。

    :param file_name: 要查询的文件名（不包括扩展名）
    :param table: 要查询的数据表名
    """
    save_dir = "save_dir"
    os.makedirs(save_dir, exist_ok=True)

    try:
        # 从数据库中获取文件信息（这里假设 get_file_from_mysql 返回一个生成器）
        for result in get_file_from_mysql(file_name, table):
            filename, blob, md5sum = result
            print(f"Filename: {filename}")

            # 提取文件名和扩展名
            file_base, file_ext = os.path.splitext(filename)

            # 构建新的文件路径和名称
            file_path = os.path.join(save_dir, f"{file_base}_{md5sum}{file_ext}")

            # 将文件内容写入到本地文件中
            with open(file_path, 'wb') as f:
                f.write(blob)

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':
    # write_file_form_mysql('baoshu', 'phone')
    for i in get_file_from_mysql('82061201.txt','guhua'):
        print(i[0],i[2])
