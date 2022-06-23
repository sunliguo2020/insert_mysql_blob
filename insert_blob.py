# -*- coding: utf-8 -*-
"""
@author: sunliguo
@contact: QQ376440229
@Created on: 2022/3/13 22:03


保存文件到mysql中，
    id,file_name,content,md5sum,blob,mod_time

2022-03-18:只能保存65535字节 原来是sql 字段类型改为 mediumblob
            max_allowed_packet = 10M
2022-04-08：统计插入失败的文件（后来注释了）
2022-06-04:使用git统一管理，以后就不会很混乱了。
            准备增加logging

遗留问题：
    1、线程池似乎没有起作用。
    2、插入长文件名的时候，没有插入成功，但是没有报错？
"""
import hashlib
import os
import os.path
import time
import logging
import traceback
import json
from sun_tool.db import db
from sun_tool.dir_walk import dir_walk
from concurrent.futures import ThreadPoolExecutor, as_completed

'''
debug,info,warning,error,critical
'''
logging.basicConfig(filename='insert_blob.log',
                    level=logging.DEBUG,
                    filemode='w',
                    encoding='utf-8',
                    format='%(asctime)s-%(filename)s[line:%(lineno)d]-%(message)s')


def file_blob(filename):
    """
    返回文件的二进制
    :param filename:
    :return:
    """
    if os.path.isfile(filename):

        with open(filename, 'rb') as f:
            blob = f.read()
        return blob
    else:
        return None


def file_md5sum(filename):
    """
    返回文件的md5值
    :param filename:
    :return:
    """
    with open(filename, 'rb') as fp:
        f_content = fp.read()
        fmd5 = hashlib.md5(f_content)
    return fmd5.hexdigest()


def file_modtime(filename):
    """
    返回文件的修改时间
    :param filename:
    :return:
    """
    file_modify_time = time.strftime("%Y-%m-%d %H:%M:%S",
                                     time.localtime(os.path.getmtime(filename)))
    return file_modify_time


def check_del(file_path, md5sum, table=''):
    """
    检查数据表中是否有该文件，如果有则删除
    :param file_path:
    :param md5sum:
    :param table:
    :return: 数据库中没有该文件则返回None，有并且删除了返回1.
    """
    file_name = os.path.basename(file_path)
    sql = f'select md5sum from {table} where `md5sum` =%(md5sum)s and `file_name` = %(file_name)s;'
    result = db.fetch_one(sql, md5sum=md5sum, file_name=file_name)
    if result is not None:

        logging.info(f'{file_name}文件已经存在！md5:{md5sum}')

        # 删除已经存在的文件
        try:
            os.remove(file_path)
            if os.path.isfile(file_path):
                logging.warning(f"{file_path}删除失败")
            else:
                logging.info(f"{file_path}删除成功")
        except Exception as e:
            # logging.error(traceback.format_exc())
            logging.error("删除出错", e)
        return 1
    else:
        logging.info(f"数据库中不存在该文件{file_name}")
        return None


def insert_blob(file_path, table=''):
    """
    把文件插入到mysql中
    :param file_path: 要插入的文件路径
    :param table:   将要插入的数据表
    :return:
    """
    # 准备材料
    file_name = os.path.basename(file_path)
    md5sum = file_md5sum(file_path)
    blob = file_blob(file_path)
    modtime = file_modtime(file_path)

    # 检查文件是否已经存在 md5sum 值相同
    logging.debug(f"查询数据库中是否有该文件:{file_name}")

    result = check_del(file_path, md5sum, table)
    if result == 1:
        logging.info(f"{file_path}有该文件并且已删除")
    elif result is None:  # 查询不到该文件，准备插入
        logging.debug(f"{file_name}查询不到该文件，准备插入")
        if len(file_name) >50:
            logging.warning(f"{file_name}文件名超出了50个字符！")
        query = f'insert into {table}  values (NULL,%s,%s,%s,%s)'
        args = (file_name, md5sum, blob, modtime)

        try:
            db.exec(query, args)
        except Exception as e:
            logging.error(file_name, "插入失败", e)

        else:  # 插入成功，准备检查并删除
            result = check_del(file_path, md5sum, table)
            if result == 1:
                logging.info(f"插入{file_path}后删除成功")
            else:
                logging.info(f'{file_path}没有插入成功')
    else:
        print("未知！")


if __name__ == '__main__':

    json_file = 'config.json'
    with open(json_file, encoding='utf-8') as fp:
        cfg = json.load(fp)

    # 导入文件所在的目录
    root_dir = cfg.get('root_dir')
    # 将要导入的数据表
    table = cfg.get('table')

    file_count = 0
    futures = []
    pool_result = []

    with ThreadPoolExecutor() as t:
        for file_path in dir_walk(root_dir):
            file_count += 1
            print(file_count, file_path)

            # 防止程序占用太高
            if file_count % 10000 == 0:
                time.sleep(30)

            # 向线程池中提交任务
            futures.append(t.submit(insert_blob, file_path, table))

        # 等待返回的结果，结果都是None，暂时没发现改怎么用
        # for future in as_completed(futures):
        #     pool_result.append(future.result())

    # print(pool_result[:20])
