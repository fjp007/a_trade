# coding: utf-8
import datetime
from a_trade.stock_minute_data import get_minute_data_for_multiple_stocks
from typing import Callable, List
from typing import Callable, List, Dict
import logging

class StockSubscriptionBus:
    """
    分钟数据订阅模块，支持多个股票代码的分钟数据订阅。
    """
    def __init__(self, trade_date):
        self.subscribers: Dict[str, Callable[[str, dict, str], None]] = {}
        self.trade_date = trade_date

    def subscribe(self, stock_codes: List[str], callback: Callable[[str, dict, str], None]):
        """
        订阅股票的分钟数据。

        参数:
            stock_codes: list，订阅的股票代码列表。
            callback: function，回调函数，接受三个参数(stock_code, minute_data, current_time)。
            trade_date: string，交易日期，例如 "20240102"。
        """
        logging.info(f"开始监听 {stock_codes}")
        for stock_code in stock_codes:
            self.subscribers[stock_code] = callback

    def unsubscribe(self, stock_code: str) -> bool:
        """
        取消订阅某只股票。

        参数:
            stock_code: string，取消订阅的股票代码。
        """
        if stock_code in self.subscribers:
            logging.info(f"停止监听 {stock_code}")
            del self.subscribers[stock_code]
            return True
        else:
            return False

    def start_trade(self, start_time="09:30", end_time="15:00", interval=1):
        """
        模拟实盘交易日，按时间间隔执行回调。

        参数:
            start_time: string，开始时间，例如 "09:30"。
            end_time: string，结束时间，例如 "15:00"。
            interval: int，时间间隔，单位为分钟。
        """
        if not self.trade_date:
            raise ValueError("交易日期未设置，请在订阅时指定 trade_date。")

        stock_codes = list(self.subscribers.keys())
        if not stock_codes:
            return

        # 获取所有订阅股票的分钟数据
        stock_minute_data = get_minute_data_for_multiple_stocks(stock_codes, self.trade_date)

        # 转换分钟数据为按时间分组的字典
        time_grouped_data = {}
        for stock_code, minute_data_list in stock_minute_data.items():
            for data in minute_data_list:
                trade_time = datetime.datetime.strptime(data.trade_time, "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
                if trade_time not in time_grouped_data:
                    time_grouped_data[trade_time] = {}
                time_grouped_data[trade_time][stock_code] = data

        # 按分钟模拟推送数据
        current_time = datetime.datetime.strptime(start_time, "%H:%M")
        end_time = datetime.datetime.strptime(end_time, "%H:%M")
        minute_interval = datetime.timedelta(minutes=interval)

        while current_time <= end_time:
            trade_time = current_time.strftime("%H:%M")
            if trade_time in time_grouped_data:
                for stock_code, data in time_grouped_data[trade_time].items():
                    if not self.subscribers:
                        return
                    if stock_code in self.subscribers:
                        self.subscribers[stock_code](stock_code, data, trade_time)

            current_time += minute_interval
