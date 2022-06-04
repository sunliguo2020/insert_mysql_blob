# -*- coding: utf-8 -*-
"""
@author: sunliguo
@contact: QQ376440229
@Created on: 2022/3/13 22:03

CREATE TABLE `sgy_idcard_pic` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `file_name` char(50) CHARACTER SET latin1 NOT NULL,
      `md5sum` char(32) CHARACTER SET latin1 NOT NULL DEFAULT '',
      `blob` mediumblob NOT NULL,
      `mod_time` datetime NOT NULL,
      PRIMARY KEY (`id`),
      KEY `md` (`md5sum`) USING HASH,
      KEY `fn` (`file_name`) USING HASH
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;

保存文件到mysql中，
    id,file_name,content,md5sum,blob,mod_time

2022-03-18:只能保存65535字节 原来是sql 字段类型改为 mediumblob
            max_allowed_packet = 10M
2022-04-08：统计插入失败的文件
2022-06-04:使用git
"""
import hashlib
import os
import os.path
import sys
import time
import traceback
from sun_tool.db import db

from concurrent.futures import ThreadPoolExecutor


def file_blob(filename):
    with open(filename, 'rb') as f:
        blob = f.read()
    return blob


def file_md5sum(filename):
    with open(filename, 'rb') as fp:
        f_content = fp.read()
        fmd5 = hashlib.md5(f_content)
    return fmd5.hexdigest()


def file_modtime(filename):
    """

    :param filename:
    :return:
    """
    file_modify_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(filename)))
    return file_modify_time

def insert_blob(file_path, table='', database='crawl'):
    """
    把文件插入到mysql中
    :param file_path: 要插入的文件路径
    :param database:   将要插入的数据库
    :param table:   将要插入的数据表
    :return:
    """
    # 准备
    file_name = os.path.basename(file_path)
    md5sum = file_md5sum(file_path)
    blob = file_blob(file_path)
    modtime = file_modtime(file_path)

    # 检查文件是否已经存在 md5sum 值相同，并且文件名相同
    # 有些文件虽然文件名一样但是md5值可以不同
    # 文件名和MD5值都一样的情况：
    sql = f"select md5sum from {table} where `md5sum` = '{md5sum}' and `file_name` ='{file_name}';"

    # print("select md5sum from %(table)s where `md5sum` =%(md5sum)s and `file_name` = %(file_name)s;"%{"table":table,"md5sum":md5sum,"file_name":file_name})
    result = db.fetch_one("select md5sum from DaglPerson where `md5sum` =%(md5sum)s and `file_name` = %(file_name)s;",
                          table=table, md5sum=md5sum, file_name=file_name)
    # result = db.fetch_one(sql)
    if result is not None:

        print(f'{file_name}文件已经存在！md5:{md5sum}')

        # 删除已经存在的文件
        try:
            os.remove(file_path)
            if os.path.isfile(file_path):
                print(f"{file_path}删除失败")
            else:
                print(f"{file_path}删除成功")
        except Exception as e:
            print(traceback.format_exc())
            print("删除出错", e)

    # 插入
    else:
        # INSERT INTO `file` (`id`, `file_name`, `md5sum`, `mod_time`) VALUES ('1', '1', '1', '2022-03-13 22:54:42')
        query = f'insert into {table}  values (NULL,%s,%s,%s,%s)'
        # print(modtime)
        args = (file_name, md5sum, blob, modtime)
        # print(args)
        try:
            db.execute(query, args)
            # conn.commit()
            # print(f'{file_name}插入成功！')
        except Exception as e:
            print(file_name, "插入失败！", e)
            # with open(insert_file_failed, 'a', encoding='utf-8') as f:
            #     f.write(str(insert_time) + f' {file_name} 插入失败 {e} ' + "\n")


if __name__ == '__main__':
    # 导入文件所在的目录
    root_dir = r'F:\pycharm\ShouGuangYun\jiankang\DaglPerson'
    # 将要导入的数据表
    table = 'DaglPerson'

    if not os.path.isdir(root_dir):
        print(root_dir, "不是一个目录")
        sys.exit(-1)
    file_count = 0

    with ThreadPoolExecutor(max_workers=100) as t:
        for root, dirs, files in os.walk(root_dir):
            for file in files:
                file_count += 1
                file_path = os.path.join(root, file)
                print(file_count, file_path)

                t.submit(insert_blob, file_path, table)
                # insert_blob(file_path, table=table)
