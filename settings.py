# -*- coding: utf-8 -*-
"""
@author: sunliguo
@contact: QQ376440229
@Created on: 2022/7/3 20:48
"""


class Settings:
    """存储所有设置的类"""

    def __init__(self):
        """初始化的设置"""
        self.root_dir = r"Z:\爬虫\网页抓取\资料\guhua\0533"
        self.table = "guhua"

        """数据库链接信息"""
        self.user = "root"
        self.password = "admin"
        self.host = "192.168.1.207"
        self.port = '3306'


if __name__ == '__main__':
    mysettings = Settings()
    print(mysettings.user)
    print(dir(mysettings))