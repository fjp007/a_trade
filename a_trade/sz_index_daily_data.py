# coding: utf-8
# 文件：sz_index_daily_data.py
import sys
import logging
from a_trade.index_daily_data import update_index_data_until
from a_trade.db_base import Base

def update_sz_index_daily_data_until(end_date):
    logging.info(f"正在获取截止至 {end_date} 的上证指数数据")
    update_index_data_until(ts_code='000001.SH', end_date=end_date, table_name='sz_index_daily_data')

def main():
    if len(sys.argv) != 2:
        print("用法: python sh_index_data.py <结束日期(yyyyMMdd)>")
        sys.exit(1)
    
    end_date = sys.argv[1]
    update_index_data_until(end_date)

if __name__ == "__main__":
    main()