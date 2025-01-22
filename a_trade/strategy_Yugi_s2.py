# coding: utf-8
from typing import Optional, Callable, List, Tuple, Type, Dict, Any
import logging
import datetime
from pydantic import Field
from sqlalchemy.orm.attributes import flag_modified
from a_trade.trade_calendar import TradeCalendar
from a_trade.db_base import Session
from a_trade.stocks_daily_data import get_stock_daily_data_for_day
from a_trade.limit_up_data_tushare import LimitUpTushare, LimitDataSource
from a_trade.stock_minute_data import get_minute_data
from a_trade.strategy import (
    StrategyParams, StrategyTask, StrategyTaskMode, Strategy, StrategyObservationEntry,
    ObservationVariable, SubscribeStopType, SellInfoRecord, BuyInfoRecord, StrategySession, ObservationVarMode, TradeRecord)

class StrategyParamsYugiS2(StrategyParams):
    # 冰点线定义，高于这条线，不做弱转强
    open_time_threshold: int = Field(
        "14:00", 
        description="一字板炸板最早时间线"
    )

    # 涨停幅度下限，影响卖出策略。
    limit_up_pch: float = Field(
        9.8, 
        description="涨停幅度下限，影响买入策略。"
    )

strategy_params = StrategyParamsYugiS2()

class ObservedStockS2Model(ObservationVarMode):
    """
    ObservedStockS1Model 属性定义，使用 Pydantic 验证。
    """
    first_open_time: Optional[str] = Field(None, description="一字板首次砸开时间")
    next_high: Optional[float] = Field(0, description="第二天最高价")
    next_close: Optional[float] = Field(0, description="第二天收盘价")
    is_limit_up: Optional[bool] = Field(False, description="第二天涨停")

class StrategyTaskYugiS2(StrategyTask):
    """
    策略任务类，用于封装日内策略操作, 包含准备观察池、订阅分时、取消订阅、买入股票、卖出股票。
    """

    def schedule_task(self):
        limit_data_source = LimitDataSource(self.trade_date)
        for stock_code, stock_limit_info in limit_data_source.limit_failed_map.items():
            daily_data = limit_data_source.get_daily_data(stock_code)
            if daily_data.open == daily_data.high and (daily_data.high/daily_data.pre_close - 1)*100 > strategy_params.limit_up_pch and daily_data.close < daily_data.pre_close:
                first_open_time = None
                minutes_data = get_minute_data(stock_code, self.trade_date)
                for minute_data in minutes_data:
                    if not first_open_time and minute_data.high < daily_data.high:
                        first_open_time = minute_data.trade_time
                        break
                last_data = minutes_data[-1]
                if datetime.datetime.strptime(first_open_time, "%Y-%m-%d %H:%M:%S").time() > datetime.datetime.strptime(strategy_params.open_time_threshold, "%H:%M").time():
                    pre_date = TradeCalendar.get_previous_trade_date(self.trade_date)
                    pre_limit_data_source = LimitDataSource(pre_date)
                    if stock_code in pre_limit_data_source.limit_up_map:
                        pre_limit_info = pre_limit_data_source.limit_up_map[stock_code]
                        if pre_limit_info.continuous_limit_up_count > 1:
                            model = ObservedStockS2Model(first_open_time = first_open_time)
                            self.add_observation_entry_with_variable(trade_date=self.trade_date, stock_code=stock_code, stock_name=stock_limit_info.stock_name, variables=model.model_dump())
                            self.buy_stock(stock_code=stock_code, buy_price=daily_data.close, buy_time=last_data.trade_time)

    def prepare_observed_pool(self):
        is_trade_date = super().prepare_observed_pool()
        if is_trade_date:
            for stock_code, record in self.observe_stocks_to_sell.items():
                daily_data = get_stock_daily_data_for_day(stock_code, self.trade_date)
                if not daily_data:
                    continue

                trade_time = datetime.datetime.strptime(self.trade_date, '%Y%m%d').replace(hour=15, minute=0, second=0, microsecond=0)
                self.sell_stock(stock_code=stock_code, sell_price=daily_data.close, trade_time=trade_time)

    def daily_report(self):
        pass

    def msg_desc_from(self, stock_code: str, var: ObservationVariable) -> str:
        return ""
    
    def update_trade_data(self):
        with StrategySession() as session:
            # 获取所有相关的ObservationVariable记录
            pre_trade_date = TradeCalendar.get_previous_trade_date(self.trade_date)
            results = self.strategy.query_observation_data(session=session, include_observation_variable=True, filters=[
                StrategyObservationEntry.trade_date == pre_trade_date
            ])
                        
            for entry, var in results:
                daily_data = get_stock_daily_data_for_day(entry.stock_code, self.trade_date)
                if not daily_data:
                    entry.trade_date = TradeCalendar.get_next_trade_date(self.trade_date)
                    session.merge(entry)
                else:
                    # 显式更新数据库记录
                    is_limit_up = (daily_data.close/daily_data.pre_close - 1)*100 > strategy_params.limit_up_pch
                    var.variables['is_limit_up'] = is_limit_up
                    var.variables['next_high'] = daily_data.high
                    var.variables['next_close'] = daily_data.close
                    flag_modified(var, "variables")
                    session.add(var)
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                logging.error(f"Failed to commit status updates: {str(e)}")
                raise
    
    def trade_did_end(self):
        self.schedule_task()
        self.update_trade_data()

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
        next_limit_trades = [record[2].variables["is_limit_up"] for record in records 
                            if record[2].variables["is_limit_up"] == True]
        limit_rate = len(next_limit_trades) / len(records) if len(records) > 0 else 0
        return {
            "次日封板次数": f"{len(next_limit_trades)}",
            "次日封板率": f"{limit_rate:.2%}"
        }
    
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