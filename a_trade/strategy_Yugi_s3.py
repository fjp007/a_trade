# coding: utf-8
from typing import Optional, Callable
import logging
import datetime

from a_trade.db_base import Session
from a_trade.limit_up_data_tushare import LimitDataSource, find_recent_limit_up, get_limit_info
from a_trade.strategy_task import StrategyTask, StrategyTaskMode, Strategy
from a_trade.stock_minute_data import get_minute_data
from a_trade.stocks_daily_data import get_stock_daily_data_for_day
from a_trade.trade_calendar import TradeCalendar

class StrategyTaskYugiS3(StrategyTask):
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
            
    def sell_stock(self, stock_code: str, sold_price: float, trade_time: datetime.datetime, observer_sell_info, sell_reason: str):
        pass

class StrategyYugiS3(Strategy):
    def __init__(self, start_date, end_date, observe_pool_cls, trade_record_cl, task_cls):
        super().__init__(start_date, end_date, observe_pool_cls, trade_record_cl, task_cls)
        self.total_count = 0
        self.success_count = 0
        self.limit_count = 0
        print(f"买入日 股票名 上次连板数 今日近期涨停情况, 上次涨停近期情况 上次涨停距离今日交易日, 昨日收盘状态")

    def analysis_observed_stocks_pool_for_day(self, trade_date):
        limit_data_source = LimitDataSource(trade_date)
        for stock_code, stock_limit_info in limit_data_source.limit_up_map.items():
            daily_data = limit_data_source.get_daily_data(stock_code)
            if (daily_data.open / daily_data.pre_close - 1)*100 < -9.8 and (daily_data.close / daily_data.pre_close -1)*100 > 9.8:
                pre_limit_up = find_recent_limit_up(stock_code, trade_date, 5)
                pre_date_limit = get_limit_info(stock_code, TradeCalendar.get_previous_trade_date(trade_date))
                pre_date_status = pre_date_limit.limit_status_desc() if pre_date_limit else '未封板'
                minutes_data = get_minute_data(stock_limit_info.stock_code, stock_limit_info.trade_date)
                last_down_time = None
                for minute_data in minutes_data:
                    if (minute_data.low / daily_data.pre_close - 1)*100 < -9.8:
                        last_down_time = minute_data.trade_time
                if pre_limit_up:
                    print(f"{trade_date} {stock_limit_info.stock_name} {pre_limit_up.continuous_limit_up_count}, {stock_limit_info.up_stat}, {pre_limit_up.up_stat}, {TradeCalendar.cal_trade_days(pre_limit_up.trade_date, trade_date)}, {pre_date_status} {last_down_time}")
                    self.total_count += 1

    def analyze_strategy_performance(self):
        print(f"[出手次数] {self.total_count}")
        return super().analyze_strategy_performance()
            
    def export_trade_records_to_excel(self):
        pass


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("用法: python market_chart.py <开始日期(yyyyMMdd)> <结束日期(yyyyMMdd)>")
        sys.exit(1)
    start_date = sys.argv[1]
    end_date = sys.argv[2]

    strategy = StrategyYugiS3(start_date, end_date, None, None, None)
    strategy.analysis_observed_stocks_pool()

    strategy.start_trade_by_strategy()
    strategy.analyze_strategy_performance()
    strategy.export_trade_records_to_excel()