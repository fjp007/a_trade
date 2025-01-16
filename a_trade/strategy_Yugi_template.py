# coding: utf-8
from typing import Optional, Callable
import logging
import datetime

from a_trade.trade_calendar import TradeCalendar
from a_trade.db_base import Session
from a_trade.limit_up_data_tushare import LimitUpTushare, LimitDataSource
from a_trade.strategy_task import StrategyTask, StrategyTaskMode, Strategy

class StrategyTaskYugiS1(StrategyTask):
    """
    策略任务类，用于封装日内策略操作, 包含准备观察池、订阅分时、取消订阅、买入股票、卖出股票。
    """

    def __init__(self, trade_date: str, mode: StrategyTaskMode = StrategyTaskMode.MODE_BACKTEST_LOCAL,
                 buy_callback: Optional[Callable] = None, sell_callback: Optional[Callable] = None,
                 subscribe_callback: Optional[Callable] = None, unsubscribe_callback: Optional[Callable] = None,
                 enable_send_msg: bool = False):
        """
        初始化策略任务类。
        参数:
            trade_date: str，交易日期，例如 "20240102"。
            mode: StrategyTaskMode，运行模式
            buy_callback: 买入回调函数
            sell_callback: 卖出回调函数
            subscribe_callback: 订阅回调函数
            unsubscribe_callback: 取消订阅回调函数
            enable_send_msg: 是否发送微信通知
        """
        super().__init__(
            trade_date=trade_date,
            mode=mode,
            buy_callback=buy_callback,
            sell_callback=sell_callback,
            subscribe_callback=subscribe_callback,
            unsubscribe_callback=unsubscribe_callback,
            enable_send_msg=enable_send_msg
        )

        self.prepare_observed_pool()

    def prepare_observed_pool(self):
        pass
    
    def stop_subscribe_buy_stock(self, stock_code):
        if stock_code not in self.observe_stocks_to_sell:
            super().stop_subscribe_buy_stock(stock_code)

    def handle_buy_stock(self, stock_code: str, minute_data, trade_time: str):
        """
        处理分钟数据的回调函数。

        参数:
            stock_code: str，股票代码。
            minute_data: dict，分钟数据。
            trade_time: str，当前分钟时间。
        """
        pass

    def handle_sell_stock(self, stock_code: str, minute_data, trade_time: str):
        pass
    
    def buy_stock(self, stock_code, stock_name, buy_price, buy_time):
        pass
            
    def sell_stock(self, stock_code: str, sold_price: float, trade_time: datetime.datetime, observer_sold_info, sell_reason: str):
        pass

class StrategyYugiS1(Strategy):
    def analysis_observed_stocks_pool_for_day(self, trade_date):
        pass
            
    def export_trade_records_to_excel(self):
        pass


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("用法: python market_chart.py <开始日期(yyyyMMdd)> <结束日期(yyyyMMdd)>")
        sys.exit(1)
    start_date = sys.argv[1]
    end_date = sys.argv[2]

    strategy = StrategyYugiS1(start_date, end_date)
    strategy.analysis_observed_stocks_pool()

    strategy.start_trade_by_strategy()
    strategy.analyze_strategy_performance()
    strategy.export_trade_records_to_excel()