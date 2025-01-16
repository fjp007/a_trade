# coding: utf-8
# 文件：market_analysis.py

import logging
import datetime
import sys
from sqlalchemy import Column, String, Integer, Float, func, case
from a_trade.db_base import Session, Base
from a_trade.stocks_daily_data import get_stocks_daily_data, StockDailyData
from a_trade.trade_calendar import TradeCalendar
from a_trade.limit_up_data_tushare import LimitUpTushare, LimitDataSource
import a_trade.settings

# 定义数据库模型
class MarketDailyData(Base):
    __tablename__ = 'market_daily_data'
    trade_date = Column(String, primary_key=True)
    up_num = Column(Integer)
    down_num = Column(Integer)
    limit_up_num = Column(Integer)
    non_one_word_limit_up_num = Column(Integer)
    limit_down_num = Column(Integer)
    prev_limit_up_high_open_rate = Column(Float)
    prev_limit_up_success_rate = Column(Float)
    blow_up_rate = Column(Float)
    sentiment_index = Column(Integer)
    physical_board_next_day_limit_up_rate = Column(Float)
    limit_up_amount = Column(Float)
    highest_continuous_up_count = Column(Integer)

def _calculate_market_daily_data(trade_date):
    session = Session()
    try:
        logging.info(f"正在分析 {trade_date} 市场情绪")

        # 使用聚合查询优化
        result = session.query(
            func.sum(case((StockDailyData.pct_chg > 0, 1), else_=0)).label("up_num"),
            func.sum(case((StockDailyData.pct_chg < 0, 1), else_=0)).label("down_num"),
            func.count().label("a_total")
        ).filter(
            StockDailyData.trade_date == trade_date
        ).one()

        # 解包结果
        up_num, down_num, a_total = result.up_num, result.down_num, result.a_total

        # 计算情绪指标
        sentiment_index = round(up_num * 100 / a_total) if a_total > 0 else 0

        # 获取指定日期的所有涨停数据
        limit_data_source = LimitDataSource(trade_date)
        records = limit_data_source.all_limit_map.values()

        limit_up_num = sum(1 for row in records if row.limit_status == 'U')
        one_word_limit_up_num = sum(1 for row in records if row.limit_status == 'U' and row.open_times == 0 and int(row.first_time) <= 93000)
        non_one_word_limit_up_num = limit_up_num - one_word_limit_up_num
        limit_up_amount = sum(row.amount for row in records if row.limit_status in ('U', 'Z') and row.amount is not None)
        limit_down_num = sum(1 for row in records if row.limit_status == 'D')
        blow_up_num = sum(1 for row in records if row.limit_status == 'Z')
        highest_continuous_up_count = max((row.continuous_limit_up_count for row in records if row.limit_status == 'U'), default=0)

        # 炸板率计算
        blow_up_rate = round(blow_up_num / (blow_up_num + limit_up_num), 2) if (blow_up_num + limit_up_num) > 0 else 0

        # 获取昨日非一字涨停板
        # 初始化 LimitDataSource
        prev_trade_date = TradeCalendar.get_previous_trade_date(trade_date)
        prev_limit_data_source = LimitDataSource(prev_trade_date)
        # 从 limit_up_map 中过滤符合条件的记录
        prev_limit_up_records = [
            record for record in prev_limit_data_source.limit_up_map.values()
            if record.open_times > 0 or int(record.first_time) > 93000
        ]
        
        prev_limit_up_stock_codes = [row.stock_code for row in prev_limit_up_records]
        prev_limit_up_close_prices = {row.stock_code: row.close for row in prev_limit_up_records}

        # 获取昨日实体板股票
        prev_physical_board_stock_codes = [
            record.stock_code for record in prev_limit_data_source.limit_up_map.values()
            if int(record.first_time) > 93000
        ]

        # 调用 get_stocks_daily_data 获取日线行情数据
        stocks_data = get_stocks_daily_data(prev_limit_up_stock_codes + prev_physical_board_stock_codes, trade_date, trade_date)

        # 计算昨日打板高开率和成功率
        high_open_count = 0
        success_count = 0
        trading_stocks_count = 0
        for stock_code in prev_limit_up_stock_codes:
            if stock_code in stocks_data and trade_date in stocks_data[stock_code]:
                trading_stocks_count += 1
                today_data = stocks_data[stock_code][trade_date]
                if today_data.open > prev_limit_up_close_prices[stock_code]:
                    high_open_count += 1
                if today_data.close > prev_limit_up_close_prices[stock_code]:
                    success_count += 1
        prev_limit_up_high_open_rate = round(high_open_count / trading_stocks_count, 2) if trading_stocks_count > 0 else 0
        prev_limit_up_success_rate = round(success_count / trading_stocks_count, 2) if trading_stocks_count > 0 else 0

        # 计算实体板次日连板率
        next_day_limit_up_count = 0
        trading_stocks_count_physical = 0
        for stock_code in prev_physical_board_stock_codes:
            if stock_code in stocks_data and trade_date in stocks_data[stock_code]:
                trading_stocks_count_physical += 1
                if stock_code in limit_data_source.limit_up_map:
                    next_day_limit_up_count += 1
        logging.info(f"[分析结果] next_day_limit_up_count: {next_day_limit_up_count}")
        logging.info(f"[分析结果] trading_stocks_count_physical: {trading_stocks_count_physical}")
        physical_board_next_day_limit_up_rate = round(next_day_limit_up_count / trading_stocks_count_physical, 2) if trading_stocks_count_physical > 0 else 0

        # 打印调试信息
        logging.info(f"[分析结果] 当前交易日: {trade_date}")
        logging.info(f"上一个交易日: {prev_trade_date}")
        logging.info(f"涨停数: {limit_up_num}")
        logging.info(f"一字涨停板: {one_word_limit_up_num}")
        logging.info(f"非一字涨停板: {non_one_word_limit_up_num}")
        logging.info(f"涨停票博弈金额: {limit_up_amount}")
        logging.info(f"跌停数: {limit_down_num}")
        logging.info(f"炸板数: {blow_up_num}")
        logging.info(f"炸板率: {blow_up_rate}")
        logging.info(f"市场最高连板空间: {highest_continuous_up_count}")
        logging.info(f"昨日涨停高开率 {prev_limit_up_high_open_rate}")
        logging.info(f"昨日涨停成功率: {prev_limit_up_success_rate}")
        logging.info(f"实体板连板率: {physical_board_next_day_limit_up_rate}")
        logging.info(f"上涨数: {up_num}")
        logging.info(f"下跌数: {down_num}")
        logging.info(f"情绪指标: {sentiment_index}")

        return {
            'trade_date': trade_date,
            'up_num': up_num,
            'down_num': down_num,
            'limit_up_num': limit_up_num,
            'non_one_word_limit_up_num': non_one_word_limit_up_num,
            'limit_up_amount': limit_up_amount,
            'limit_down_num': limit_down_num,
            'blow_up_rate': blow_up_rate,
            'prev_limit_up_high_open_rate': prev_limit_up_high_open_rate,
            'prev_limit_up_success_rate': prev_limit_up_success_rate,
            'physical_board_next_day_limit_up_rate': physical_board_next_day_limit_up_rate,
            'sentiment_index': sentiment_index,
            'highest_continuous_up_count' : highest_continuous_up_count
        }
    finally:
        session.close()

def update_market_daily_data_during(start_date, end_date):
    logging.info(f"正在分析 从 {start_date} 到 {end_date} 的市场情绪")
    if not  TradeCalendar.validate_date_range(start_date, end_date):
        return
    
    market_daily_data_list = []
    trade_days = TradeCalendar.get_trade_dates(start_date, end_date)
    for current_date in trade_days:
        market_daily_data = _calculate_market_daily_data(current_date)
        market_daily_data_list.append(market_daily_data)

    write_market_daily_data_to_db_batch(market_daily_data_list)
    logging.info(f"市场情绪数据从 {start_date} 到 {end_date} 已写入数据库")

def write_market_daily_data_to_db_batch(market_daily_data_list):
    if len(market_daily_data_list) <= 0:
        return

    session = Session()
    try:
        for market_daily_data in market_daily_data_list:
            session.merge(MarketDailyData(**market_daily_data))
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"市场情绪数据写入失败: {e}")
    finally:
        session.close()

if __name__ == "__main__":    
    if len(sys.argv) != 3:
        print("用法: python market_analysis.py <开始日期(yyyyMMdd)> <结束日期(yyyyMMdd)>")
        sys.exit(1)
    
    update_market_daily_data_during(sys.argv[1], sys.argv[2])
