# -*- coding: utf-8 -*-
'''
 @Time : 2022/6/4 21:57
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
 @File : test.py
 @Project : pycharm
'''
'''
放一些测试的代码
'''
# from sun_tool.db import db
import json


# from insert_blob import file_md5sum

# print(file_md5sum('13001530199.txt'))
# if __name__ == '__main__':
#     print(db)
#     db.fetch_one("select count(*) from token_file;",())
# json_file = 'config.json'
# with open(json_file,encoding='utf-8') as fp:
#     cfg = json.load(fp)
#
# print(cfg)
# print(type(cfg))
import glob

print(glob.iglob('d:\pycharm'))
for i in glob.iglob('d:\*'):
    print(i)
    # break