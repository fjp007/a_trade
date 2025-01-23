# coding: utf-8
# stock_base.py

import logging
from a_trade.settings import _get_tushare
from sqlalchemy import Column, String
from a_trade.db_base import Session, Base
from a_trade.trade_calendar import TradeCalendar
from a_trade.stocks_daily_data import StockDailyData

# 定义数据库模型
class StockBase(Base):
    __tablename__ = 'stock_base'
    ts_code = Column(String, primary_key=True)
    symbol = Column(String)
    name = Column(String)
    area = Column(String)
    industry = Column(String)
    market = Column(String)
    exchange = Column(String)
    list_status = Column(String)
    list_date = Column(String)
    delist_date = Column(String)
    is_hs = Column(String)


#  获取新股信息
def update_new_stocks(recent_trade_date: str):
        # 获取最近一个交易日
    # recent_trade_date = TradeCalendar.get_recent_trade_date()
    logging.info(f"获取 {recent_trade_date} 的新股信息")

    session = Session()
    try:
        # 从股票日线数据库中获取该交易日所有股票代码
        trade_stocks = session.query(StockDailyData.ts_code).filter(
            StockDailyData.trade_date == recent_trade_date
        ).distinct().all()
        trade_stocks = {stock.ts_code for stock in trade_stocks}

        # 从 stock_base 数据表中筛选出没有的股票代码
        existing_stocks = session.query(StockBase.ts_code).all()
        existing_stocks = {stock.ts_code for stock in existing_stocks}

        new_stocks = trade_stocks - existing_stocks

        # 请求 Tushare 接口获取新股信息
        if new_stocks:
            logging.info(f"在 {recent_trade_date} 交易的股票中，共有 {len(new_stocks)} 支新股: {new_stocks}")
            new_stocks_data = _get_tushare().pro_api().stock_basic(
                ts_code=','.join(new_stocks),
                fields='ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,is_hs'
            )
            for _, row in new_stocks_data.iterrows():
                new_record = StockBase(
                    ts_code=row['ts_code'],
                    symbol=row['symbol'],
                    name=row['name'],
                    area=row['area'],
                    industry=row['industry'],
                    market=row['market'],
                    exchange=row['exchange'],
                    list_status=row['list_status'],
                    list_date=row['list_date'],
                    delist_date=row.get('delist_date', None),
                    is_hs=row['is_hs']
                )
                session.merge(new_record)
            session.commit()
        else:
            logging.info("没有需要更新的新股信息")
    except Exception as e:
        session.rollback()
        logging.error(f"新股信息更新失败: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    import sys
