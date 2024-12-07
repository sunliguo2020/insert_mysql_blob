# -*- coding: utf-8 -*-
"""
 @Time : 2024/12/7 21:20
 @Author : sunliguo
 @Email : sunliguo2006@qq.com
"""
import os
import logging.config
import yaml


def setup_logging(default_path='logging.yaml',
                  default_level=logging.INFO,
                  env_key='LOG_CFG'):
    """Setup logging configuration"""
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value

    if os.path.exists(path):
        with open(path, 'r') as f:
            config = yaml.safe_load(f.read())
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
        print(f"No logging configuration file found at {path}, using default settings.")


if __name__ == '__main__':
    # 在项目的入口点（如main.py）调用这个函数
    import os
    from logging_config import setup_logging

    setup_logging()
