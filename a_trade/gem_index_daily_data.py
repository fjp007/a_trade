# coding: utf-8
# 文件：gem_index_daily_data.py
import sys
import logging
from a_trade.index_daily_data import update_index_data_until, update_index_data

def update_gem_index_daily_data_until(end_date):
    logging.info(f"正在获取截止至 {end_date} 的创业板指数数据")
    update_index_data_until(ts_code='399006.SZ', end_date=end_date, table_name='gem_index_daily_data')

def main():
    if len(sys.argv) != 2:
        print("用法: python gem_index_data.py <结束日期(yyyyMMdd)>")
        sys.exit(1)
    
    end_date = sys.argv[1]
    update_gem_index_daily_data_until(end_date)

if __name__ == "__main__":
    main()

    