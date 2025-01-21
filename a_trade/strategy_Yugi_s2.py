# coding: utf-8
from typing import Optional, Callable, List, Tuple, Type, Dict, Any
import logging
import datetime
from pydantic import Field
from a_trade.trade_calendar import TradeCalendar
from a_trade.db_base import Session
from a_trade.limit_up_data_tushare import LimitUpTushare, LimitDataSource
from a_trade.strategy import (
    StrategyParams, StrategyTask, StrategyTaskMode, Strategy, StrategyObservationEntry,
    ObservationVariable, SubscribeStopType, SellInfoRecord, BuyInfoRecord, StrategySession, ObservationVarMode, TradeRecord)

class StrategyParamsYugiS2(StrategyParams):
    # 冰点线定义，高于这条线，不做弱转强
    cold_sentiment_line: int = Field(
        40, 
        description="冰点线定义，高于这条线，不做弱转强"
    )

strategy_params = StrategyParamsYugiS2()

class ObservedStockS2Model(ObservationVarMode):
    """
    ObservedStockS1Model 属性定义，使用 Pydantic 验证。
    """
    concept_name: Optional[str] = Field(None, description="概念名称")

class StrategyYugiS2(Strategy):
    def __init__(self, start_date, end_date, observe_pool_cls, trade_record_cl, task_cls):
        super().__init__(start_date, end_date, observe_pool_cls, trade_record_cl, task_cls)
        self.total_count = 0
        self.success_count = 0
        self.limit_count = 0
        print(f"买入日 股票名 买入日收盘价 次日最高价 次日收盘价")

    def analysis_observed_stocks_pool_for_day(self, trade_date):
        limit_data_source = LimitDataSource(trade_date)
        for stock_code, stock_limit_info in limit_data_source.limit_failed_map.items():
            daily_data = limit_data_source.get_daily_data(stock_code)
            if daily_data.open == daily_data.high and (daily_data.high/daily_data.pre_close - 1)*100 > 9.8 and daily_data.close < daily_data.pre_close:
                first_open_time = None
                minutes_data = get_minute_data(stock_code, trade_date)
                for minute_data in minutes_data:
                    if not first_open_time and minute_data.high < daily_data.high:
                        first_open_time = minute_data.trade_time
                        break
                if datetime.datetime.strptime(first_open_time, "%Y-%m-%d %H:%M:%S").time() > datetime.time(14, 00):
                    pre_date = TradeCalendar.get_previous_trade_date(trade_date)
                    pre_limit_data_source = LimitDataSource(pre_date)
                    if stock_code in pre_limit_data_source.limit_up_map:
                        pre_limit_info = pre_limit_data_source.limit_up_map[stock_code]
                        if pre_limit_info.continuous_limit_up_count > 1:
                            self.total_count += 1
                            next_daily_data = get_stock_daily_data_for_day(stock_code, TradeCalendar.get_next_trade_date(trade_date))
                            
                            if next_daily_data.close > next_daily_data.pre_close:
                                self.success_count += 1
                                print(f"[成功] {trade_date} {stock_limit_info.stock_name} {stock_limit_info.close} {next_daily_data.high} {next_daily_data.close} {first_open_time}")
                            else:
                                print(f"[失败] {trade_date} {stock_limit_info.stock_name} {stock_limit_info.close} {next_daily_data.high} {next_daily_data.close} {first_open_time}")
                            if (next_daily_data.close/next_daily_data.pre_close - 1) * 100 > 9.8:
                                self.limit_count += 1
    def analyze_strategy_performance(self):
        success_rate = self.success_count / self.total_count
        limit_rate = self.limit_count / self.total_count
        print(f"一字板14:00后开板买入次数: {self.total_count}")
        print(f"买入次日最高点大于买入日收盘价次数: {self.success_count}")
        print(f"次日封板次数: {self.limit_count}")
        print(f"成功率: {success_rate:.2%}")
        print(f"次日封板率: {limit_rate:.2%}")
        return super().analyze_strategy_performance()

class StrategyTaskYugiS2(StrategyTask):
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

    def daily_report(self):
        pass

    def msg_desc_from(self, stock_code: str) -> str:
        return ""
    
    def trade_did_end(self):
        pass

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
    
class StrategyYugiS2(Strategy):
    def strategy_name(self) -> str:
        return "Yugi_s2"
    
    def get_observation_model(self) -> ObservationVarMode:
        return ObservedStockS2Model
    
    def analyze_performance_datas(self, records: List[Tuple[TradeRecord, StrategyObservationEntry, ObservationVariable]]) -> Dict[str, Any]:
        return {}
    
    def __init__(self, task_cls: StrategyTask = StrategyTaskYugiS2, params: Type['StrategyParams'] = StrategyParamsYugiS2(), mode: StrategyTaskMode = StrategyTaskMode.MODE_BACKTEST_LOCAL):
        super().__init__(task_cls, params, mode)


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("用法: python market_chart.py <开始日期(yyyyMMdd)> <结束日期(yyyyMMdd)>")
        sys.exit(1)
    start_date = sys.argv[1]
    end_date = sys.argv[2]

    strategy = StrategyYugiS2()
    # strategy.publish(clear=True)
    strategy.local_simulation(start_date, end_date)
    strategy.analyze_strategy_performance(start_date, end_date)
    strategy.export_trade_records_to_excel(start_date, end_date)