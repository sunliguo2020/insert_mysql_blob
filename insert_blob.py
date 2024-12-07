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
import os.path
import time
import logging

# 获取一个日志记录器
logger = logging.getLogger('my_project')

from sun_tool.dir_walk import dir_walk
from concurrent.futures import ThreadPoolExecutor, as_completed

'''
debug,info,warning,error,critical
'''


# logging.basicConfig(filename='insert_blob.log',
#                     level=logging.DEBUG,
#                     filemode='w',
#                     encoding='utf-8',
#                     format='%(asctime)s-%(filename)s[line:%(lineno)d]-%(message)s')


class FileProcessor:
    """

    """

    def __init__(self, settings, db):
        self.settings = settings
        self.db = db
        self.file_path_list = []

    def process_file(self, file_path):
        try:
            file_name = os.path.basename(file_path)
            blob, md5sum = self._get_file_blob_md5sum(file_path)
            modtime = self._get_file_modtime(file_path)

            if self._check_and_delete_existing_file(file_path, md5sum):
                logger.info(f"{file_path}已存在并删除")
                return

            if len(file_name) > 50:
                logger.warning(f"{file_name}文件名超出了50个字符！")

            self._insert_file_into_db(file_name, md5sum, blob, modtime)
            logger.info(f"{file_path}插入成功")

        except Exception as e:
            logger.error(f"处理文件{file_path}时出错: {e}")

    def _get_file_blob_md5sum(self, file_path):
        """
                返回文件的二进制和md5值
        """
        with open(file_path, 'rb') as fp:
            f_content = fp.read()
            fmd5 = hashlib.md5(f_content).hexdigest()
        return f_content, fmd5

    def _get_file_modtime(self, file_path):
        """
        返回文件的修改时间
        @param file_path:
        @return:
        """
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(file_path)))

    def _check_and_delete_existing_file(self, file_path, md5sum):
        file_name = os.path.basename(file_path)
        query = f'SELECT md5sum FROM {self.settings.table} WHERE md5sum=%s AND file_name=%s'
        # result = self.db.fetch_one(query, file_name=file_name, md5sum=md5sum)
        result = self.db.fetch_one(query, md5sum=md5sum, file_name=file_name)
        if result:
            try:
                os.remove(file_path)
                logger.info(f"{file_path}删除成功")
                return True
            except Exception as e:
                logger.error(f"删除文件{file_path}时出错: {e}")
        return False

    def _insert_file_into_db(self, file_name, md5sum, blob, modtime):
        query = f'INSERT INTO {self.settings.table} VALUES (NULL,%s,%s,%s,%s)'
        self.db.exec(query, (file_name, md5sum, blob, modtime))

    def run(self):
        with ThreadPoolExecutor() as executor:
            future_to_file = {executor.submit(self.process_file, file_path): file_path for file_path in
                              dir_walk(self.settings.root_dir)}
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"处理文件{file_path}时出错: {e}")


if __name__ == '__main__':
    pass
