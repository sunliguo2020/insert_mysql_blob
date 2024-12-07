# -*- coding: utf-8 -*-
"""
 @Time : 2022/6/5 17:44
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
 @File : dir_walk.py
 @Project : pycharm
"""
import os
import glob


# 遍历文件夹，返回里面每个文件的路径。
# 下一步，支持通配符
# 找到了一个库glob 这个是查找匹配文件的
def dir_walk(file_dir):
    """
    递归遍历文件夹，并生成所有文件的路径。
    :param file_dir:要遍历的文件夹路径
    :return:
    """
    if file_dir is None:
        raise TypeError("file_dir should be a string, bytes, os.PathLike or integer, not NoneType")
    if not os.path.isdir(file_dir):
        raise NotADirectoryError(f"{file_dir} is not a directory")

    for root, dirs, files in os.walk(file_dir):
        for file in files:
            file_path = os.path.join(root, file)
            yield file_path


if __name__ == '__main__':
    for i in dir_walk(r'f:\github\*'):
        print(i)
