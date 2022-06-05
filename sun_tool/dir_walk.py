# -*- coding: utf-8 -*-
'''
 @Time : 2022/6/5 17:44
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
 @File : dir_walk.py
 @Project : pycharm
'''
import os
def dir_walk(file_dir):
    for root,dirs,files in os.walk(file_dir):
        for file in files:
            file_path =os.path.join(root,file)
            yield  file_path