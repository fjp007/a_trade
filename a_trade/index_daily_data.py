# coding: utf-8
# index_daily_data.py

import logging
import datetime
from a_trade.settings import _get_tushare
from sqlalchemy import Column, String, Float, create_engine
from a_trade.db_base import Session, get_recent_trade_date_in_table, Base, engine
from a_trade.trade_calendar import TradeCalendar

# 缓存类的字典
_index_daily_data_classes = {}

def get_index_daily_data_class(table_name):
    if table_name in _index_daily_data_classes:
        return _index_daily_data_classes[table_name]

    class SIndexDailyData(Base):
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
    _index_daily_data_classes[table_name] = SIndexDailyData
    return SIndexDailyData

# 获取指数日线
def update_index_data(ts_code, start_date, end_date, table_name):
    SIndexDailyData = get_index_daily_data_class(table_name)
    logging.info(f"正在获取从 {start_date} - {end_date} 的指数 {ts_code} 日线")
    index_data = _get_tushare().pro_api().index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    logging.debug(index_data)

    session = Session()
    try:
        for _, row in index_data.iterrows():
            new_record = SIndexDailyData(
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

Base.metadata.create_all(engine)

def merge_to_index_daily_data():
    gem_class = get_index_daily_data_class("gem_index_daily_data")
    sci_tech_class = get_index_daily_data_class("sci_tech_50_index_daily_data")
    sz_class = get_index_daily_data_class("sz_index_daily_data")
    with Session() as session:
        try:
            # 查询三张表的数据
            gem_data = session.query(gem_class).all()
            sci_tech_data = session.query(sci_tech_class).all()
            sz_data = session.query(sz_class).all()

            # 合并数据
            all_data = gem_data + sci_tech_data + sz_data

            # 插入到 index_daily_data 表
            for record in all_data:
                # 创建新表的实例
                new_record = IndexDailyData(
                    ts_code=record.ts_code,
                    trade_date=record.trade_date,
                    close=record.close,
                    open=record.open,
                    high=record.high,
                    low=record.low,
                    pre_close=record.pre_close,
                    change=record.change,
                    pct_chg=record.pct_chg,
                    vol=record.vol,
                    amount=record.amount
                )
                # 添加到会话
                session.add(new_record)
            
            # 提交事务
            session.commit()
            print("数据合并完成！")
        except Exception as e:
            session.rollback()
            print(f"数据合并失败: {e}")
        finally:
            session.close()

if __name__ == "__main__":
    # update_index_data_until('000001.SH', '20241101', 'index_daily_data')
    merge_to_index_daily_data()
    with Session() as session:
        merged_data = session.query(IndexDailyData).all()
        print(f"合并后的数据条目数: {len(merged_data)}")
    