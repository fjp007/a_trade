# coding: utf-8
from typing import Optional, Callable, List, Tuple, Type, Dict, Any
import logging
import datetime
from sqlalchemy.orm.attributes import flag_modified
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

class StrategyTaskYugiS2(StrategyTask):
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