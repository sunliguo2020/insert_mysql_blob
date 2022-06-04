# -*- coding: utf-8 -*-
'''
 @Time : 2022/6/4 21:57
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
 @File : test.py
 @Project : pycharm
'''
from sun_tool.db import db

if __name__ == '__main__':
    print(db)
    db.fetch_one("select count(*) from token_file;",())