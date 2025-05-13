# -*- coding: utf-8 -*-
"""
 @Time : 2024/12/7 21:22
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
"""
from logging_config import setup_logging

setup_logging()

from insert_blob import FileProcessor
from settings import Settings
from sun_tool.db import DBHelper

if __name__ == '__main__':
    settings = Settings(root_dir=r"X:\targz\0635",
                        table='guhua')
    db = DBHelper(host=settings.host,
                  port=settings.port,
                  user=settings.user,
                  password=settings.password)

    processor = FileProcessor(settings, db)
    processor.run()
