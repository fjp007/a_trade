# coding: utf-8
# 文件：stocks_daily_data.py

import datetime
import logging
from a_trade.settings import _get_tushare
from sqlalchemy import Column, String, Float
from a_trade.db_base import Session, get_recent_trade_date_in_table, Base
from a_trade.trade_calendar import TradeCalendar
from a_trade.stock_minute_data import get_minute_data

t_limit_open_condition = 7
    

# 定义数据库模型
class StockDailyData(Base):
    __tablename__ = 'stocks_daily_data'
    ts_code = Column(String, primary_key=True)
    trade_date = Column(String, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    pre_close = Column(Float)
    change = Column(Float)
    pct_chg = Column(Float)
    vol = Column(Float)
    amount = Column(Float)

    def __repr__(self):
        return f"<StockDailyData(stock_code={self.ts_code}, close={self.close}, pre_close={self.pre_close})>"

    def is_t_limit(self):
        # avg_price_pch = (self.amount * 1000 /self.vol)/self.pre_close - 100
        if (self.open*100 / self.pre_close - 100) > t_limit_open_condition and (self.high - 0.02 <= self.close) and  self.open - self.low > (self.close - self.open)*0.6:
            minute_datas = get_minute_data(self.ts_code, self.trade_date)
            first_limit_time = None
            for data in minute_datas:
                if not first_limit_time and (data.high*100 / self.pre_close - 100) > 9.7:
                    first_limit_time = data.trade_time
                if first_limit_time and data.trade_time > first_limit_time and data.low < self.high:
                    # 封板后再开板
                    return True
        return False
    
    def is_one_limit(self):
        return self.high == self.low and self.high > self.pre_close
    
class StockDailyDataSource():
    def __init__(self, stock_code, end_date, previous_delta):
        self.stock_code = stock_code
        self.end_date = end_date
        self.previous_delta = previous_delta

        with Session() as session:
            datas = session.query(
                    StockDailyData
                ).filter(
                    StockDailyData.ts_code == stock_code,
                    StockDailyData.trade_date < end_date
                ).order_by(
                    StockDailyData.trade_date.desc()
                ).limit(previous_delta).all()
            self.daily_data_cache = {
                data.trade_date: data
                for data in datas
            }

    def is_one_limit(self):
        for daily_data in self.daily_data_cache.values():
            if not daily_data.is_one_limit():
                return False
            
        return True
    
    def get_highest_vol(self):
        highest_vol = 0
        for data in self.daily_data_cache.values():
            if highest_vol < data.vol:
                highest_vol = data.vol
        return highest_vol
    
    def get_lastest_date_vol(self):
        """
        获取 daily_data_cache 中离 end_date 最近的交易日(小于 end_date)的交易量。
        """
        if not self.daily_data_cache:
            # 如果没有数据，可以返回 0 或 None，视业务需要决定
            return 0
        
        # 1. 找出所有日期中最大的 trade_date（也就是最接近 end_date 的日期）
        latest_trade_date = max(self.daily_data_cache.keys())
        
        # 2. 返回对应的 vol
        return self.daily_data_cache[latest_trade_date].vol


def get_stock_daily_data_for_day(stock_code, trade_date) -> StockDailyData:
    result = get_stocks_daily_data([stock_code], trade_date, trade_date)
    if stock_code in result and trade_date in result[stock_code]:
        return result[stock_code][trade_date]
    logging.warning(f"{stock_code} 于交易日{trade_date} 未找到日线数据，可能停牌")
    return None

# 获取指定股票指定日期范围内数据
def get_stocks_daily_data(stock_codes, start_date, end_date):
    with Session() as session:
        stock_codes = list(set(stock_codes))  # 确保股票代码唯一
        logging.debug(f"去重后,即将从数据表获取{len(stock_codes)} 个股票日线数据")

        # 使用一次查询代替原来的循环查询
        records = session.query(StockDailyData).filter(
            StockDailyData.ts_code.in_(stock_codes),
            StockDailyData.trade_date.between(start_date, end_date)
        ).all()

        stocks_data = {}
        not_found_stock_codes = set(stock_codes)  # 初始化未找到的股票代码集合

        for record in records:
            stock_code = record.ts_code
            if stock_code not in stocks_data:
                stocks_data[stock_code] = {}
            stocks_data[stock_code][record.trade_date] = record
            not_found_stock_codes.discard(stock_code)

        # 记录未找到数据的股票代码
        if not_found_stock_codes:
            logging.warning(f"Not found stock codes: {not_found_stock_codes}, 该股票可能停牌")

        return stocks_data

def update_stocks_daily_data(trade_date):
    session = Session()
    try:
        # 打印日志说明正在拉取交易数据的日期
        logging.debug(f"正在拉取交易数据的日期: {trade_date}")
        daily_data = _get_tushare().pro_api().daily(trade_date=trade_date)

        if daily_data.empty:
            logging.warning(f"没有获取到交易数据: {trade_date}")
        else:
            logging.info(f"获取到的数据数量: {len(daily_data)}，交易日期: {trade_date}")
            # 插入获取的数据
            for _, row in daily_data.iterrows():
                new_record = StockDailyData(
                    ts_code=row['ts_code'],
                    trade_date=row['trade_date'],
                    open=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
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
        logging.error(f"数据写入失败: {e}")
    finally:
        session.close()

def update_stocks_daily_data_until(end_date):
    logging.debug(f"正在拉取截止{end_date}的日线数据")
    start_date = get_recent_trade_date_in_table(StockDailyData.__tablename__)
    # 获取期间所有交易日期
    trade_dates = TradeCalendar.get_trade_dates(start_date, end_date)

    # 循环调用update_stocks_daily_data
    for trade_date in trade_dates:
        update_stocks_daily_data(trade_date)

def get_previous_trade_date(trade_date, stock_code):
    """
    获取某只股票在指定交易日之前的最近一个有效交易日。

    参数:
        trade_date (str): 当前交易日，格式为 'YYYYMMDD'。
        stock_code (str): 股票代码，格式如 '600519.SZ'。

    返回:
        str: 股票的前一个有效交易日，格式为 'YYYYMMDD'。
    """
    try:
        # 初始化数据源，仅获取 1 天的数据
        data_source = StockDailyDataSource(stock_code, trade_date, previous_delta=1)
        
        # 获取缓存中的交易日期
        trade_dates = list(data_source.daily_data_cache.keys())
        
        if trade_dates:
            # 返回最近的交易日（因为 previous_delta=1，所以必定是最近一天）
            return trade_dates[0]
        else:
            # 没有找到有效交易日
            logging.warning(f"股票 {stock_code} 在 {trade_date} 之前没有有效交易记录。")
            return None

    except Exception as e:
        logging.error(f"获取前一个有效交易日失败: {e}")

if __name__ == "__main__":
    # import sys
    # if len(sys.argv) != 2:
    #     print("用法: python concepts_daily_data.py <开始日期(yyyyMMdd)> <结束日期(yyyyMMdd)>")
    #     sys.exit(1)
    # trade_date = sys.argv[1]
    # end_date = sys.argv[2]
    # update_stocks_daily_data(trade_date)
    df = _get_tushare().pro_api().daily(trade_date='20200422', ts_code='002307.SZ')
    print(df)