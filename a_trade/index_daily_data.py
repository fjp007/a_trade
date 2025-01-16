# coding: utf-8
# index_daily_data.py

import logging
import datetime
from a_trade.settings import _get_tushare
from sqlalchemy import Column, String, Float
from a_trade.db_base import Session, get_recent_trade_date_in_table, Base
from a_trade.trade_calendar import TradeCalendar

# 缓存类的字典
_index_daily_data_classes = {}

def get_index_daily_data_class(table_name):
    if table_name in _index_daily_data_classes:
        return _index_daily_data_classes[table_name]

    class IndexDailyData(Base):
        __tablename__ = table_name
        ts_code = Column(String, primary_key=True)
        trade_date = Column(String, primary_key=True)
        close = Column(Float)
        open = Column(Float)
        high = Column(Float)
        low = Column(Float)
        pre_close = Column(Float)
        change = Column(Float)
        pct_chg = Column(Float)
        vol = Column(Float)
        amount = Column(Float)
    
    # 缓存类
    _index_daily_data_classes[table_name] = IndexDailyData
    return IndexDailyData

# 获取指数日线
def update_index_data(ts_code, start_date, end_date, table_name):
    IndexDailyData = get_index_daily_data_class(table_name)
    logging.info(f"正在获取从 {start_date} - {end_date} 的指数 {ts_code} 日线")
    index_data = _get_tushare().pro_api().index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    logging.debug(index_data)

    session = Session()
    try:
        for _, row in index_data.iterrows():
            new_record = IndexDailyData(
                ts_code=row['ts_code'],
                trade_date=row['trade_date'],
                close=row['close'],
                open=row['open'],
                high=row['high'],
                low=row['low'],
                pre_close=row['pre_close'],
                change=row['change'],
                pct_chg=row['pct_chg'],
                vol=row['vol'],
                amount=row['amount']
            )
            session.merge(new_record)
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"指数日线数据更新失败: {e}")
    finally:
        session.close()

def update_index_data_until(ts_code, end_date, table_name):
    start_date = get_recent_trade_date_in_table(table_name, 'trade_date', '20140101')
    if not  TradeCalendar.validate_date_range(start_date, end_date):
        return

    update_index_data(ts_code, start_date, end_date, table_name)

if __name__ == "__main__":
    update_index_data_until('000001.SH', '20241101', 'index_daily_data')
    