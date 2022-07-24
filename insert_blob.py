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
2022-06-13:日志中记录本次处理到的文件个数
2022-07-03:添加设置类

遗留问题：
    1、线程池似乎没有起作用。
    2、插入长文件名的时候，没有插入成功，但是没有报错？原因是文件名截断后插入了。
"""
import hashlib
import os
import sys
import os.path
import time
import logging
from sun_tool.db import db
from sun_tool.dir_walk import dir_walk
from concurrent.futures import ThreadPoolExecutor, as_completed
from settings import Settings

'''
debug,info,warning,error,critical
'''
logging.basicConfig(filename='insert_blob.log',
                    level=logging.DEBUG,
                    filemode='w',
                    encoding='utf-8',
                    format='%(asctime)s-%(filename)s[line:%(lineno)d]-%(message)s')

def file_blob_md5sum(filename):
    """
        返回文件的二进制和md5值
    """
    try:
        with open(filename, 'rb') as fp:
            f_content = fp.read()
            fmd5 = hashlib.md5(f_content)
    except FileNotFoundError:
        logging.debug(f"{filename} is not found!")
    except PermissionError:
        logging.debug(f"{filename} PermissionError")
    except Exception as e:
        logging.debug("in file_blob" ,e)
    else:
        return f_content, fmd5.hexdigest()


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

    # md5sum = file_md5sum(file_path)
    # blob = file_blob(file_path)
    # if blob is None:
    #     return
    result = file_blob_md5sum(file_path)
    if result is not None:
        blob, md5sum = result
    else:
        sys.exit(1)

    modtime = file_modtime(file_path)

    # 记录处理文件的个数
    logging.debug(f'已经处理完文件个数/剩余文件总数：{file_count - len(file_path_list)}/{len(file_path_list)}')

    # 检查文件是否已经存在 md5sum 值相同
    logging.debug(f"查询数据库中是否有该文件:{file_name}")

    result = check_del(file_path, md5sum, table)
    if result == 1:
        logging.info(f"{file_path}有该文件并且已删除")
        file_path_list.remove(file_path)
    elif result is None:  # 查询不到该文件，准备插入
        logging.debug(f"{file_name}查询不到该文件，准备插入")
        if len(file_name) > 50:
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
                file_path_list.remove(file_path)
            else:
                logging.info(f'{file_path}没有插入成功')
    else:
        print("未知！")


if __name__ == '__main__':

    settings = Settings()

    file_count = 0
    futures = []
    pool_result = []

    # 记录要处理的文件列表
    file_path_list = []

    with ThreadPoolExecutor() as t:
        for file_path in dir_walk(settings.root_dir):
            file_count += 1
            print(file_count, file_path)
            file_path_list.append(file_path)

            # 防止程序占用太高
            if len(file_path_list) > 10000:
                time.sleep(len(file_path_list) / 1000)

            # 向线程池中提交任务
            futures.append(t.submit(insert_blob, file_path, settings.table))
