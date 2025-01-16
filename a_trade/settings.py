# coding: utf-8
# settings.py

import tushare as ts
import os
import logging
import datetime
from dotenv import load_dotenv
from pathlib import Path
import matplotlib.font_manager as fm
import sys

# 统一设置标准输出和错误输出的编码为 UTF-8
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# 加载环境变量
load_dotenv()

def get_project_path() -> Path:
    project_dir = os.getenv("PROJECT_DIR")
    if not project_dir:
        logging.error("PROJECT_DIR 环境变量未设置。")
        raise ValueError("PROJECT_DIR 环境变量未设置。")
    return Path(project_dir)

def configure_logging(ENV=os.getenv('ENV', 'dev')):
    """
    配置日志记录，根据环境变量决定输出到文件还是控制台
    """
    # 设置日志格式
    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    
    if ENV == 'release' or ENV == 'test':
        # 生产环境下，日志写入文件
        log_dir = get_project_path() / 'log'
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"log_{datetime.datetime.now().strftime('%Y%m%d')}.log"
        
        # 配置日志写入文件和控制台
        logging.basicConfig(level=logging.INFO,
                            format=log_format,
                            handlers=[
                                logging.FileHandler(log_path, encoding='utf-8'),
                                logging.StreamHandler()  # 控制台输出
                            ])
    else:
        # 开发环境下，日志输出到控制台
        logging.basicConfig(level=logging.INFO,
                            format=log_format,
                            handlers=[logging.StreamHandler()])  # 控制台输出
    
    logging.info(f"当前环境为: {ENV}")

configure_logging()

def get_chinese_font():
    # 设置字体，确保支持中文显示
    return fm.FontProperties(fname=os.getenv('FONT_PATH'))

def _get_tushare():
    # 设置 Tushare token
    token = os.getenv("TUSHARE_TOKEN")
    # token = "b111d910691a06e215fc33dcbeccbfdfc409ab3fde7b4d2fdc49727b"
    if not token:
        raise ValueError("TUSHARE_TOKEN not found in environment variables.")
    ts.set_token(token)
    return ts
