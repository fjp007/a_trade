# coding: utf-8
import datetime
import logging
from typing import Callable, List, Dict
from collections import defaultdict

from a_trade.stock_minute_data import get_minute_data_for_multiple_stocks

class TimeScheduleBus:
    """
    分钟数据订阅模块，支持多个股票代码的分钟数据订阅，
    并支持在模拟时间触发自定义回调(函数)。
    由内部自动决定 start_time 与 end_time，不再需要外部传入。
    """

    def __init__(self, trade_date: str):
        self.subscribers: Dict[str, Callable[[str, dict, datetime.datetime], None]] = {}
        self.trade_date = trade_date
        
        # 用于保存“定时函数调用”：键为 datetime.datetime 对象，值为回调函数列表
        self.scheduled_func_calls = defaultdict(list)

    def subscribe(self, stock_codes: List[str], callback: Callable[[str, dict, datetime.datetime], None]):
        """
        订阅股票的分钟数据。

        参数:
            stock_codes: list，订阅的股票代码列表。
            callback: function，回调函数，接受三个参数 (stock_code, minute_data, current_time)。
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

    def register_api_call(self, trigger_time: str, callback: Callable[[], None]):
        """
        注册一个在指定时间触发执行的回调函数。

        参数:
            trigger_time: str，触发时间，格式可以是"HH:MM"或"HH:MM:SS"，
                         仅模拟09:00:00 - 20:00:00之间的时间有效，超出范围将抛出异常。
            callback: Callable，无参数的回调函数或可调用对象。

        说明:
            - 内部以分钟粒度为主（若需要秒级可自行扩展）。
            - 若多次在同一时间点注册不同回调函数，所有回调都会依次执行。
        """
        # 先将 trigger_time 统一转换为 datetime.datetime 对象
        fmt = "%H:%M" if len(trigger_time) == 5 else "%H:%M:%S"
        try:
            time_part = datetime.datetime.strptime(trigger_time, fmt).time()
        except ValueError:
            raise ValueError(f"触发时间格式错误: {trigger_time}, 请使用HH:MM或HH:MM:SS格式")

        # 检查范围 09:00:00 <= dt_time <= 20:00:00
        if time_part < datetime.time(9, 0, 0) or time_part > datetime.time(20, 0, 0):
            raise ValueError("触发时间超出允许范围(09:00:00 - 20:00:00)")

        # 转换为 trade_date 的 datetime.datetime 对象
        trigger_datetime = datetime.datetime.combine(
            datetime.datetime.strptime(self.trade_date, "%Y%m%d").date(),
            time_part
        )

        self.scheduled_func_calls[trigger_datetime].append(callback)
        
        # 移除对 scheduled_func_calls 进行重新赋值和排序的操作
        # self.scheduled_func_calls = dict(sorted(self.scheduled_func_calls.items()))
        
        func_name = getattr(callback, '__name__', repr(callback))
        logging.info(f"已注册定时函数，时间={trigger_datetime.time()}, 函数名={func_name}")

    def start_trade(self, interval: int = 1):
        """
        启动交易仿真，依次执行盘前、盘中、盘后交易。
        """
        self.pre_market_trade()
        self.during_market_trade(interval=interval)
        self.post_market_trade()
    
    def pre_market_trade(self):
        """
        盘前交易：
        执行所有在09:30之前注册的API回调。
        """
        trade_date_obj = datetime.datetime.strptime(self.trade_date, "%Y%m%d").date()
        pre_market_end = datetime.datetime.combine(trade_date_obj, datetime.time(9, 30))

        # 获取所有触发时间在09:30之前的回调，并按时间排序
        pre_market_callbacks = []
        for trigger_time, callbacks in sorted(self.scheduled_func_calls.items()):
            if trigger_time < pre_market_end:
                pre_market_callbacks.extend(callbacks)
                del self.scheduled_func_calls[trigger_time]

        for callback in pre_market_callbacks:
            func_name = getattr(callback, '__name__', repr(callback))
            logging.info(f"{self.trade_date} 即将执行盘前回调函数: {func_name}")
            try:
                callback()
            except Exception as e:
                logging.error(f"执行盘前回调函数异常: {e}")

    def during_market_trade(self, interval: int = 1):
        """
        盘中交易：
        如果存在订阅股票代码或盘中回调，09:30 - 15:00通过current_dt模拟时间按照分钟推进，
        在每个时间点执行注册的回调并推送分钟数据给订阅者。
        """
        if not self.trade_date:
            raise ValueError("交易日期未设置，请在初始化时指定 trade_date。")

        trade_date_obj = datetime.datetime.strptime(self.trade_date, "%Y%m%d").date()

        market_start_datetime = datetime.datetime.combine(trade_date_obj, datetime.time(9, 30))
        market_end_datetime = datetime.datetime.combine(trade_date_obj, datetime.time(15, 0))

        # 获取盘中所有触发时间，并按时间排序
        market_callbacks = defaultdict(list)
        for trigger_time, callbacks in sorted(self.scheduled_func_calls.items()):
            if market_start_datetime <= trigger_time <= market_end_datetime:
                market_callbacks[trigger_time].extend(callbacks)

        # 移除盘中的回调
        for trigger_time in list(market_callbacks.keys()):
            del self.scheduled_func_calls[trigger_time]

        # 获取股票分钟数据
        stock_codes = list(self.subscribers.keys())
        
        # 如果没有订阅股票且没有盘中回调，则跳过盘中交易
        if not stock_codes and not market_callbacks:
            logging.info(f"{self.trade_date} 盘中交易阶段没有订阅股票且没有注册的回调函数，跳过盘中交易。")
            return

        stock_minute_data = get_minute_data_for_multiple_stocks(stock_codes, self.trade_date)

        # 按 datetime.datetime 分组
        time_grouped_data = defaultdict(dict)
        for stock_code, minute_data_list in stock_minute_data.items():
            for data in minute_data_list:
                trade_datetime = datetime.datetime.strptime(data.trade_time, "%Y-%m-%d %H:%M:%S")
                trade_datetime = datetime.datetime.combine(trade_date_obj, trade_datetime.time())
                time_grouped_data[trade_datetime][stock_code] = data

        current_dt = market_start_datetime
        step = datetime.timedelta(minutes=interval)

        while current_dt <= market_end_datetime:
            # 执行在当前时间的盘中回调
            if current_dt in market_callbacks:
                for callback in market_callbacks[current_dt]:
                    func_name = getattr(callback, '__name__', repr(callback))
                    logging.info(f"{self.trade_date} [{current_dt.time()}] 即将执行盘中回调函数: {func_name}")
                    try:
                        callback()
                    except Exception as e:
                        logging.error(f"执行盘中回调函数异常: {e}")

            # 推送当前时间的股票数据给订阅者
            if current_dt in time_grouped_data:
                for stock_code, data in time_grouped_data[current_dt].items():
                    if stock_code not in self.subscribers:
                        continue
                    self.subscribers[stock_code](stock_code, data, current_dt.strftime("%H:%M"))

            current_dt += step

    def post_market_trade(self):
        """
        盘后交易：
        执行所有在15:00之后注册的API回调。
        """
        trade_date_obj = datetime.datetime.strptime(self.trade_date, "%Y%m%d").date()
        post_market_start = datetime.datetime.combine(trade_date_obj, datetime.time(15, 0))

        # 获取所有触发时间在15:00之后的回调，并按时间排序
        post_market_callbacks = []
        for trigger_time, callbacks in sorted(self.scheduled_func_calls.items()):
            if trigger_time >= post_market_start:
                post_market_callbacks.extend(callbacks)
                del self.scheduled_func_calls[trigger_time]

        for callback in post_market_callbacks:
            func_name = getattr(callback, '__name__', repr(callback))
            logging.info(f"{self.trade_date} 即将执行盘后回调函数: {func_name}")
            try:
                callback()
            except Exception as e:
                logging.error(f"执行盘后回调函数异常: {e}")
