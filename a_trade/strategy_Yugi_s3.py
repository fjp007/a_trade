# coding: utf-8
from typing import Optional, Callable, List, Tuple, Type, Dict, Any
import logging
import datetime
from sqlalchemy.orm.attributes import flag_modified
from pydantic import Field
from a_trade.trade_calendar import TradeCalendar
from a_trade.db_base import Session
from a_trade.limit_up_data_tushare import LimitUpTushare, LimitDataSource, find_recent_limit_up, get_limit_info
from a_trade.stock_minute_data import get_minute_data
from a_trade.strategy import (
    StrategyParams, StrategyTask, StrategyTaskMode, Strategy, StrategyObservationEntry,
    ObservationVariable, SubscribeStopType, SellInfoRecord, BuyInfoRecord, StrategySession, ObservationVarMode, TradeRecord)

class StrategyParamsYugiS3(StrategyParams):
    # 冰点线定义，高于这条线，不做弱转强
    cold_sentiment_line: int = Field(
        40, 
        description="冰点线定义，高于这条线，不做弱转强"
    )

strategy_params = StrategyParamsYugiS3()

class ObservedStockS3Model(ObservationVarMode):
    """
    ObservedStockS1Model 属性定义，使用 Pydantic 验证。
    """
    concept_name: Optional[str] = Field(None, description="概念名称")
    
class StrategyTaskYugiS3(StrategyTask):
    """
    策略任务类，用于封装日内策略操作, 包含准备观察池、订阅分时、取消订阅、买入股票、卖出股票。
    """

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
    
class StrategyYugiS3(Strategy):
    def strategy_name(self) -> str:
        return "Yugi_S3"
    
    def get_observation_model(self) -> ObservationVarMode:
        return ObservedStockS3Model
    
    def analyze_performance_datas(self, records: List[Tuple[TradeRecord, StrategyObservationEntry, ObservationVariable]]) -> Dict[str, Any]:
        return {}
    
    def __init__(self, task_cls: StrategyTask = StrategyTaskYugiS3, params: Type['StrategyParams'] = StrategyParamsYugiS3(), mode: StrategyTaskMode = StrategyTaskMode.MODE_BACKTEST_LOCAL):
        super().__init__(task_cls, params, mode)
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

    def analysis_stocks_during(self, start_date, end_date):
        TradeCalendar.iterate_trade_days(start_date, end_date, self.analysis_observed_stocks_pool_for_day)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("用法: python market_chart.py <开始日期(yyyyMMdd)> <结束日期(yyyyMMdd)>")
        sys.exit(1)
    start_date = sys.argv[1]
    end_date = sys.argv[2]

    strategy = StrategyYugiS3()
    # strategy.publish(clear=True)
    strategy.analysis_stocks_during(start_date, end_date)
    # strategy.analyze_strategy_performance(start_date, end_date)
    # strategy.export_trade_records_to_excel(start_date, end_date)
