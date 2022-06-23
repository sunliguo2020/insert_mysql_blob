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


# 遍历文件夹，返回里面每个文件的路径。
# 下一步，支持通配符
# 找到了一个库glob 这个是查找匹配文件的
def dir_walk(file_dir):
    """
    遍历文件夹，支持通配符
    :param file_dir:
    :return:
    """

    for item in glob.iglob(file_dir, recursive=True):
        print(item)

        if os.path.isdir(item):

            for root, dirs, files in os.walk(item):
                for file in files:
                    file_path = os.path.join(root, file)
                    yield file_path


if __name__ == '__main__':
    for i in dir_walk(r'd:\software\*'):
        print(i)
