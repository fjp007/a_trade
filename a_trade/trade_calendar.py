# coding: utf-8
# 文件：trade_calendar.py

import os
import datetime
from a_trade.settings import _get_tushare
from sqlalchemy import Column, String, Integer
from a_trade.db_base import Session, Base
import logging
from typing import Optional

# 定义数据库模型
class TradeCalendar(Base):
    __tablename__ = 'trade_calendar'
    exchange = Column(String, primary_key=True)
    cal_date = Column(String, primary_key=True)
    is_open = Column(Integer)
    pretrade_date = Column(String)

    @staticmethod
    def validate_date_range(start_date, end_date):
        """验证日期范围的有效性"""
        if datetime.datetime.strptime(start_date, "%Y%m%d") > datetime.datetime.strptime(end_date, "%Y%m%d"):
            logging.warning(f"开始日期 {start_date} 晚于结束日期 {end_date}, 无效参数")
            return False
        return True

    @staticmethod
    def update_trade_calendar(end_date=None):
        end_date = end_date or datetime.datetime.now().strftime("%Y%m%d")

        session = Session()
        try:
            # 获取最大交易日期
            max_trade_date = session.query(TradeCalendar).filter(TradeCalendar.exchange == 'SSE').order_by(TradeCalendar.cal_date.desc()).limit(1).first()
            if max_trade_date:
                start_date = (datetime.datetime.strptime(max_trade_date.cal_date, "%Y%m%d") + datetime.timedelta(days=1)).strftime("%Y%m%d")
            else:
                start_date = '20140101'

            if not TradeCalendar.validate_date_range(start_date, end_date):
                logging.info(f"trade_calendar表里未记录的日期最早起始于{start_date}，晚于{end_date}, 无需更新交易日历")
                return

            # 获取交易日历数据
            trade_calendar = _get_tushare().pro_api().trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)

            if trade_calendar.empty:
                logging.warning(f"从 {start_date} 到 {end_date} 没有获取到新的交易日历数据")
            else:
                # 插入或更新交易日历数据
                for _, row in trade_calendar.iterrows():
                    new_record = TradeCalendar(
                        exchange=row['exchange'],
                        cal_date=row['cal_date'],
                        is_open=row['is_open'],
                        pretrade_date=row['pretrade_date']
                    )
                    session.merge(new_record)

                session.commit()
                logging.info(f"成功更新了 {len(trade_calendar)} 条交易日历数据")
        except Exception as e:
            session.rollback()
            logging.error(f"更新交易日历失败: {e}")
        finally:
            session.close()
        
        logging.info(f"更新交易日历至 {end_date}")

    @staticmethod
    def get_next_trade_date(start_date: str, delta: int = 1, exchange: str = 'SSE') -> Optional[str]:
        """
        获取指定日期之后的第 delta 个交易日。

        :param start_date: 开始日期 (格式: YYYYMMDD)
        :param delta: 向前第 delta 个交易日
        :param exchange: 交易所 (默认 'SSE')
        :return: 第 delta 个交易日的日期，或者 None
        """
        with Session() as session:
            # 查询未来可能的交易日列表
            future_trade_days = (
                session.query(TradeCalendar.cal_date)
                .filter(TradeCalendar.cal_date > start_date, TradeCalendar.exchange == exchange)
                .filter(TradeCalendar.is_open == 1)  # 只选择开市日
                .order_by(TradeCalendar.cal_date.asc())
                .limit(delta)  # 只查询 delta 个交易日
                .all()
            )

            # 确保有足够的交易日返回
            if len(future_trade_days) < delta:
                return None

            # 返回目标交易日
            return future_trade_days[-1][0]  # 返回第 delta 个交易日

    @staticmethod
    def get_previous_trade_date(trade_date, delta=1, exchange='SSE'):
        """
        获取指定交易日之前的第 delta 个交易日。

        :param trade_date: 当前交易日 (格式: YYYYMMDD)
        :param delta: 向前第 delta 个交易日
        :param exchange: 交易所 (默认 'SSE')
        :return: 第 delta 个交易日的日期
        """
        with Session() as session:
            # 一次性获取所有小于 trade_date 的交易日，按日期降序排列
            results = (
                session.query(TradeCalendar.cal_date)
                .filter(TradeCalendar.cal_date < trade_date, TradeCalendar.exchange == exchange, TradeCalendar.is_open == 1)
                .order_by(TradeCalendar.cal_date.desc())
                .limit(delta)  # 只获取 delta 个交易日
                .all()
            )
            
            # 确保查询到足够的交易日
            if len(results) < delta:
                raise ValueError("Insufficient previous trade dates found for the given trade date.")
            
            # 返回第 delta 个交易日
            return results[-1][0]  # results 是 [(cal_date,)] 格式，取第一个值

    @staticmethod
    def is_trade_day(cal_date, exchange='SSE'):
        session = Session()
        try:
            result = session.query(TradeCalendar.is_open).filter(
                TradeCalendar.cal_date == cal_date,
                TradeCalendar.exchange == exchange
            ).first()
            if result:
                return result.is_open == 1
            else:
                return None
        finally:
            session.close()

    @staticmethod
    def get_trade_dates(start_date, end_date, exchange='SSE'):
        if not TradeCalendar.validate_date_range(start_date, end_date):
            return []
        
        session = Session()
        try:
            result = session.query(TradeCalendar.cal_date).filter(
                TradeCalendar.exchange == exchange,
                TradeCalendar.is_open == 1,
                TradeCalendar.cal_date.between(start_date, end_date)
            ).order_by(TradeCalendar.cal_date).all()
            return [row.cal_date for row in result]
        finally:
            session.close()

    @staticmethod
    def iterate_trade_days(start_date, end_date, closure, reverse=False):
        if not TradeCalendar.validate_date_range(start_date, end_date):
            return
        trade_days = TradeCalendar.get_trade_dates(start_date, end_date)
        if reverse:
            trade_days = trade_days[::-1]  # 倒序处理
        for current_date in trade_days:
            closure(current_date)

    @staticmethod
    def cal_trade_days(start_date: str, end_date: str) -> int:
        """
        计算指定日期范围内的交易日数量。

        :param start_date: 开始日期 (格式: YYYYMMDD)
        :param end_date: 结束日期 (格式: YYYYMMDD)
        :return: 交易日数量
        """
        # 获取交易日列表并计算数量
        trade_days = TradeCalendar.get_trade_dates(start_date, end_date)
        return len(trade_days) - 1

    @staticmethod
    def get_recent_trade_date(exchange='SSE'):
        session = Session()
        try:
            result = session.query(TradeCalendar.cal_date).filter(
                TradeCalendar.exchange == exchange,
                TradeCalendar.is_open == 1
            ).order_by(TradeCalendar.cal_date.desc()).limit(1).first()
            if result:
                return result.cal_date
            else:
                raise ValueError("No recent trade date found.")
        finally:
            session.close()

def main(today=None):
    # 获取今天日期
    if not today:
        today = datetime.datetime.now().strftime("%Y%m%d")
    print(f"更新{today}交易日历")
    # 更新交易日历
    TradeCalendar.update_trade_calendar(today)

if __name__ == "__main__":
    import sys
    main()
    sys.exit(0)