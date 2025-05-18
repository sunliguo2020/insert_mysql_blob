# -*- coding: utf-8 -*-
"""
@author: sunliguo
@contact: QQ376440229
@Created on: 2022/7/3 20:48
"""


class Settings:
    """存储所有设置的类"""

    def __init__(self,
                 root_dir=None,
                 table=None,
                 user='root',
                 password='admin',
                 host='192.168.110.207',
                 port=3306):
        """初始化的设置,允许传递参数来自定义设置"""
        self.root_dir = root_dir
        self.table = table

        """数据库链接信息"""
        self.user = user
        self.password = password
        self.host = host
        self.port = port


if __name__ == '__main__':
    mysettings = Settings()
    print(mysettings.user)
    print(dir(mysettings))
