# -*- coding: utf-8 -*-
'''
 @Time : 2022/6/5 17:44
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
 @File : dir_walk.py
 @Project : pycharm
'''
import os
import glob
#遍历文件夹，返回里面每个文件的路径。
#下一步，支持通配符
#找到了一个库gl0b
def dir_walk(file_dir):
    """
    遍历文件夹，支持通配符
    :param file_dir:
    :return:
    """
    for item in glob.iglob(file_dir):
        if os.path.isdir(item):
            for root, dirs, files in os.walk(file_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    yield file_path
