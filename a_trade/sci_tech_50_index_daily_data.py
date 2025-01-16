# coding: utf-8
# sci_tech_50_index_daily_data.py
import sys
from a_trade.index_daily_data import update_index_data, update_index_data_until
import logging

def update_sci_tech_50_index_daily_data_until(end_date):
    logging.info(f"正在获取截止 {end_date} 科创50日线数据")
    return update_index_data_until(ts_code='000688.SH', end_date=end_date, table_name='sci_tech_50_index_daily_data')

def main():
    if len(sys.argv) != 2:
        print("用法: python sci_tech_50_index_data.py <结束日期(yyyyMMdd)>")
        sys.exit(1)

    end_date = sys.argv[1]
    update_sci_tech_50_index_daily_data_until(end_date)

if __name__ == "__main__":
    main()
