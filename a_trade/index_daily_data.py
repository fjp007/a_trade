# coding: utf-8
# index_daily_data.py

import logging
from a_trade.settings import _get_tushare
from sqlalchemy import Column, String, Float
from a_trade.db_base import Session, Base
from a_trade.trade_calendar import TradeCalendar

class IndexDailyData(Base):
    __tablename__ = "index_daily_data"
    
    ts_code = Column(String, primary_key=True)  # 股票代码
    trade_date = Column(String, primary_key=True)  # 交易日期
    close = Column(Float)  # 收盘价
    open = Column(Float)  # 开盘价
    high = Column(Float)  # 最高价
    low = Column(Float)  # 最低价
    pre_close = Column(Float)  # 前收盘价
    change = Column(Float)  # 涨跌额
    pct_chg = Column(Float)  # 涨跌幅
    vol = Column(Float)  # 成交量
    amount = Column(Float)  # 成交金额
# 获取指数日线
def update_index_data(ts_code: str, start_date: str, end_date: str) -> None:
    """更新指定指数在指定日期范围内的日线数据
    
    Args:
        ts_code (str): 指数代码
        start_date (str): 开始日期，格式为YYYYMMDD
        end_date (str): 结束日期，格式为YYYYMMDD
    """
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

def update_index_data_until(end_date: str) -> None:
    """更新所有主要指数数据直到指定日期
    
    Args:
        end_date (str): 结束日期，格式为YYYYMMDD
    """
    code_map = {
        "399006.SZ": "创业板指数",
        "000688.SH": "科创50",
        "000001.SH": "上证指数"
        }
    for ts_code, desc in code_map.items():
        logging.info(f"正在获取截止至 {end_date} 的 {desc}数据")
        # 获取每个ts_code下数据库记录的最新交易日
        session = Session()
        try:
            # 直接查询该指数的最新交易日期
            recent_date = session.query(IndexDailyData.trade_date)\
                .filter(IndexDailyData.ts_code == ts_code)\
                .order_by(IndexDailyData.trade_date.desc())\
                .first()
            recent_date = recent_date[0] if recent_date else '20140101'
        finally:
            session.close()
            
        # 获取下一个交易日作为请求的start_date
        start_date = TradeCalendar.get_next_trade_date(recent_date)
        if not TradeCalendar.validate_date_range(start_date, end_date):
            return

        update_index_data(ts_code, start_date, end_date)