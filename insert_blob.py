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
from datetime import datetime

# 获取一个日志记录器
logger = logging.getLogger('my_project')

from sun_tool.dir_walk import dir_walk
from concurrent.futures import ThreadPoolExecutor, as_completed


class FileInfo:
    """
    包含处理文件的基本信息
    """

    def __init__(self, file_path):
        """
        初始化文件信息，计算文件的md5、二进制内容和修改时间
        @param file_path: 文件路径
        """
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.blob = self._get_file_blob()
        self.md5sum = self._calculate_md5()
        self.modtime = self._get_file_modtime()

    def _get_file_blob(self):
        """返回文件的二进制内容"""
        with open(self.file_path, 'rb') as fp:
            return fp.read()

    def _calculate_md5(self):
        """计算文件的md5值"""
        return hashlib.md5(self.blob).hexdigest()

    def _get_file_modtime(self):
        """返回文件的修改时间"""
        return time.strftime("%Y-%m-%d %H:%M:%S",
                             time.localtime(os.path.getmtime(self.file_path)))


class FileProcessor:
    """
    处理文件
    1、处理文件并存入数据库
    2、处理文件名超长和重复文件的情况

    """

    def __init__(self, settings, db):
        """

        @param settings: 保存要处理的文件的目录、数据库等信息
        @param db: DBHelper对象
        """
        self.settings = settings
        self.db = db
        self.file_path_list = []

    def process_file(self, file_path):
        """
        处理文件
        @param file_path:文件路径
        """
        logger.info(f"准备处理文件：{file_path}")
        try:
            # 创建FileInfo对象获取文件信息
            file_info = FileInfo(file_path)

            if len(file_info.file_name) > 50:
                logger.warning(f"{file_info.file_name}文件名超出了50个字符！")

            self._insert_or_update_file_in_db(file_info)
            logger.info(f"{file_path}处理成功")
            # 插入成功后删除该文件
            self._delete_file(file_path)

        except Exception as e:
            logger.error(f"处理文件{file_path}时出错: {e}")

    def _check_existing_file(self, file_info):
        """
        检查数据库中是否已经存在该文件，并返回现有记录的modtime
        @param file_info: FileInfo对象
        @return:
        """
        logger.debug(f"准备检查文件{file_info.file_name}是否在数据库中存在")
        query = f'SELECT mod_time FROM {self.settings.table} WHERE md5sum=%s AND file_name=%s'
        result = self.db.fetch_one(query, md5sum=file_info.md5sum, file_name=file_info.file_name)
        if result:
            logger.info(f"{file_info.file_path}已经在数据库中存在!")
            return True, result[0]
        else:
            logger.info(f"{file_info.file_path}没有在数据库中发现该文件")
            return False, None

    def _delete_file(self, file_path):
        """
        删除指定的文件，并返回操作是否成功。
        """
        try:
            os.remove(file_path)
            logger.info(f"{file_path}删除成功")
            return True
        except Exception as e:
            logger.error(f"删除文件{file_path}时出错: {e}")
            return False

    def _update_modtime_if_earlier(self, file_info, db_modtime):
        """
           如果文件的modtime早于数据库中的modtime，则更新数据库中的记录
           @param file_info: FileInfo对象
           @param db_modtime: 数据库中的modtime
           @return: 是否执行了更新
           """
        try:
            # 确保db_modtime 是有效的日期时间字符串
            if not db_modtime or not isinstance(db_modtime,str):
                return False

            file_modtime = datetime.strptime(file_info.modtime, "%Y-%m-%d %H:%M:%S")
            db_modtime = datetime.strptime(db_modtime, "%Y-%m-%d %H:%M:%S")

            if file_modtime < db_modtime:
                update_query = f'UPDATE {self.settings.table} SET modtime=%s WHERE md5sum=%s AND file_name=%s'
                self.db.exec(update_query, (file_info.modtime, file_info.md5sum, file_info.file_name))
                logger.info(f"更新文件 {file_info.file_name} 的modtime为更早的时间: {file_info.modtime}")
                return True
            return False
        except ValueError as e:
            logger.error(f"时间格式转换错误:{e},文件modtime{file_info.modtime},"
                         f"数据库modtime:{db_modtime}")
            return False

    def _insert_or_update_file_in_db(self, file_info):
        """
        将文件信息插入数据库
        @param file_info:FileInfo对象
        @return:
        """
        # 检查文件是否已经存在
        exists, db_modtime = self._check_existing_file(file_info)
        if exists:
            # 如果文件已存在，检查是否需要更新modtime
            self._update_modtime_if_earlier(file_info, db_modtime)
            return

        logger.debug(f"准备插入文件{file_info.file_name}到数据表{self.settings.table}中")

        query = f'INSERT INTO {self.settings.table} VALUES (NULL,%s,%s,%s,%s)'
        self.db.exec(query, (file_info.file_name, file_info.md5sum, file_info.blob, file_info.modtime))

    def run(self):
        with ThreadPoolExecutor(100) as executor:
            future_to_file = {executor.submit(self.process_file, file_path): file_path for file_path in
                              dir_walk(self.settings.root_dir)}
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"处理文件{file_path}时出错: {e}")
