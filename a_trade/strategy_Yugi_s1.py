# coding: utf-8
import logging
import datetime
from typing import List, Optional, Dict, Tuple, cast, Type, Any
from enum import IntFlag
from sqlalchemy import Boolean
from sqlalchemy import cast as sqlalchemy_cast
from sqlalchemy.orm.attributes import flag_modified
from pydantic import Field

from a_trade.limit_attribution import LimitDailyAttribution
from a_trade.stocks_daily_data import StockDailyData, StockDailyDataSource, get_stock_daily_data_for_day
from a_trade.trade_calendar import TradeCalendar
from a_trade.limit_up_data_tushare import LimitUpTushare, is_strong_stock_base_limit_data, LimitDataSource
from a_trade.stock_minute_data import is_strong_limit_up_base_minute_data
from a_trade.db_base import Session
from a_trade.market_analysis import MarketDailyData
import a_trade.settings
from a_trade.trade_utils import is_10cm_stock
from a_trade.wechat_bot import WechatBot
from a_trade.strategy import (
    StrategyParams, StrategyTask, StrategyTaskMode, Strategy, StrategyObservationEntry,
    ObservationVariable, SubscribeStopType, SellInfoRecord, BuyInfoRecord, StrategySession, ObservationVarMode, TradeRecord)

# 卖出策略枚举
class SellStrategyType(IntFlag):
    SellLossStopLowerThanAvgPrice = 1         # 股价低于平均成交价格线
    SellLossStopAvgPriceGoLow = 2             # 平均成交价格线走低
    SellLossStopHighThanAvgPrice = 4          # 股价高于平均成交价格线一定幅度
    SellProfitStopLowerThanAvgPrice = 8       # 股价低于于平均成交价格线一定幅度止盈
    SellProfitStopLowerThanDeadLine = 16      # 涨停/跌停，第二天卖出
    SellLossStopNeedBuyStock = 32             # 可以先卖后买，腾出仓位

# 策略参数常量
class StrategyParamsYugiS1(StrategyParams):
    # 冰点线定义，高于这条线，不做弱转强
    cold_sentiment_line: int = Field(
        40, 
        description="冰点线定义，高于这条线，不做弱转强"
    )
    
    # 板块效应下限，达到这个数量的板块不参与弱转强
    concept_effect_line: int = Field(
        2, 
        description="板块效应下限，达到这个数量的板块不参与弱转强"
    )
    
    # 板块效应连板数下限，低于这个数量不纳入观察池
    observe_min_limit_count: int = Field(
        2, 
        description="板块效应连板数下限，低于这个数量不纳入观察池"
    )

    #
    min_buy_threshold: float = Field(
        1.01,
        description="T字板买入信号最低涨幅"
    )
    
    # 非连续涨停板龙头股日均涨幅下限，影响买入股票池
    avg_recent_high_limit: float = Field(
        6.0, 
        description="非连续涨停板龙头股日均涨幅下限，影响买入股票池"
    )
    
    # 非连续涨停板龙头股总涨幅下限，影响买入股票池
    recent_high_limit: float = Field(
        50.0, 
        description="非连续涨停板龙头股总涨幅下限，影响买入股票池"
    )
    
    # 弱板开板时间下限定义(分钟)，影响买入股票池
    weak_limit_open_time: int = Field(
        10, 
        description="弱板开板时间下限定义(分钟)，影响买入股票池"
    )
    
    # 弱板股票/板块龙一 第二天纳入观察池幅度下限，影响买入策略
    buy_in_observe_dead_pch: float = Field(
        -6.0, 
        description="弱板股票/板块龙一 第二天纳入观察池幅度下限，影响买入策略"
    )
    
    # 弱板股票第一天振幅上限，影响买入策略
    first_date_amplitude: float = Field(
        15.0, 
        description="弱板股票第一天振幅上限，影响买入策略"
    )
    
    # T字板开盘幅度下限。影响买入策略
    t_limit_open_condition: float = Field(
        7.0, 
        description="T字板开盘幅度下限。影响买入策略"
    )
    
    # 第二天高开幅度上限, 影响买入策略
    next_date_open_rate_limit: float = Field(
        5.9, 
        description="第二天高开幅度上限, 影响买入策略"
    )
    
    # 烂板前连续一字板数量上限。达到这个上限，不参与弱转强买入。影响买入策略
    continuous_one_limit_count: int = Field(
        3, 
        description="烂板前连续一字板数量上限。达到这个上限，不参与弱转强买入。影响买入策略"
    )
    
    # 涨停幅度下限，影响卖出策略。
    limit_up_pch: float = Field(
        9.8, 
        description="涨停幅度下限，影响卖出策略。"
    )
    
    # 跌停幅度下限，影响卖出策略。
    limit_down_pch: float = Field(
        -9.8, 
        description="跌停幅度下限，影响卖出策略。"
    )
    
    # 前一天未吃到大面，第二天盘中分时高于黄线幅度触发卖出。影响卖出策略。3-4降低胜率，提升盈亏比
    sold_out_pch: float = Field(
        4.0, 
        description="前一天未吃到大面，第二天盘中分时高于黄线幅度触发卖出。影响卖出策略。3-4降低胜率，提升盈亏比"
    )
    
    # 止盈线
    profit_stop: float = Field(
        -6.0, 
        description="止盈线"
    )
    
    # 连续水下时长，止盈策略
    under_water_stop_minutes: int = Field(
        40, 
        description="连续水下时长，止盈策略"
    )
    
    # 水下平均成交价跌幅上限
    under_water_avg_price_pch_limit: float = Field(
        -2.0, 
        description="水下平均成交价跌幅上限"
    )
    
    # 高位板首次涨停时间卖出线，晚于这个时间涨停。板上卖出。影响卖出策略。
    high_limit_stock_first_time: str = Field(
        '14:30', 
        description="高位板首次涨停时间卖出线，晚于这个时间涨停。板上卖出。影响卖出策略。"
    )

strategy_params = StrategyParamsYugiS1()

# 策略交易股票观测数据
class ObservedStockS1Model(ObservationVarMode):
    """
    ObservedStockS1Model 属性定义，使用 Pydantic 验证。
    """
    concept_name: Optional[str] = Field(None, description="概念名称")
    concept_position: Optional[str] = Field(None, description="当日概念股中地位")
    daily_position: Optional[str] = Field(None, description="当日市场中地位")
    continuous_limit_up_count: int = Field(0, description="连续涨停次数", ge=0)
    up_stat: Optional[str] = Field(None, description="近期涨停次数统计")
    amount: Optional[float] = Field(None, description="成交额")
    turnover_ratio: Optional[float] = Field(None, description="换手率")
    recent_high_pch: float = Field(0.0, description="最近涨幅")
    recent_high_avg: float = Field(0.0, description="最近平均涨幅")
    is_t_limit: bool = Field(False, description="是否T字线")
    buy_date_status: Optional[str] = Field('未封板', description="买入日收盘状态")

    class Config:
        # 配置允许使用 JSON 数据初始化
        populate_by_name = True

# 卖出股票记录数据
class SellInfoRecordDataYugiS1():
    def __init__(self):
        self.first_limit_time = None
        self.strong_level = 0
        self.pre_avg_price = None
        self.under_water_minutes = 0
        self.under_water_avg_price_start = None
        self.pre_close = None

class StrategyTaskYugiS1(StrategyTask):
    """
    策略任务类，用于封装日内策略操作, 包含准备观察池、订阅分时、取消订阅、买入股票、卖出股票。
    """
    def schedule_task_flow(self):
        if TradeCalendar.is_trade_day(self.trade_date):
            self.schedule(closure=self.prepare_observed_pool, time="09:15:00")
            self.schedule(closure=self.trade_did_end, time="18:30:00")
            super().schedule_task_flow()
        
    def prepare_observed_pool(self):
        self.prepare_buy_pool(subscribe=True)
        self.prepare_sell_pool(subscribe=True)
        self.notice_observed_pool()

        self.pre_stock_daily_map: Dict[str, StockDailyData] = {}
        for _, sell_info_record in self.sell_info_record_map.items():
            sell_info_record.other_data = SellInfoRecordDataYugiS1()

        self.buy_var_model_map: Dict[str, ObservedStockS1Model] = {}
        for stock_code, (_, var) in self.observe_stocks_to_buy.items():
            self.buy_var_model_map[stock_code] = ObservedStockS1Model.from_observation_variable(var)

        self.pre_trade_date = TradeCalendar.get_previous_trade_date(self.trade_date)
        with Session() as session:
            stock_codes = list(self.sell_info_record_map.keys()) + list(self.buy_info_record_map.keys())
            pre_stock_daily_results = session.query(StockDailyData).filter(
                StockDailyData.ts_code.in_(stock_codes),
                StockDailyData.trade_date == self.pre_trade_date
            ).all()
            self.pre_limit_data_source = LimitDataSource(self.pre_trade_date)
            for pre_daily_data in pre_stock_daily_results:
                self.pre_stock_daily_map[pre_daily_data.ts_code] = pre_daily_data

            for stock_code in stock_codes:
                if stock_code not in self.pre_stock_daily_map:
                    last_daily_data = list(StockDailyDataSource(stock_code=stock_code, end_date=self.trade_date, previous_delta=1).daily_data_cache.values())[0]
                    self.pre_stock_daily_map[stock_code] = last_daily_data

    def daily_report(self):
        with StrategySession() as session:
            next_date = TradeCalendar.get_next_trade_date(self.trade_date)
            filters = [
                StrategyObservationEntry.trade_date == next_date,
                sqlalchemy_cast(ObservationVariable.variables['is_t_limit'], Boolean) == True  # 筛选 ObservationVariable 中 is_t_limit = True 的记录
            ]
            t_stocks = self.strategy.query_observation_data(session=session, filters=filters)

            if t_stocks:
                content = f"今日冰点，明天重点观测T字板 {','.join(list(set([stock_info.stock_name for stock_info in t_stocks])))}"
            else:
                content = "今天没有可观测的冰点T字板策略股票池"

            final_msg = (
                f"【Yugi_s1策略通知】\n"
                f" {content} "
            )
            WechatBot.send_text_msg(final_msg)

    def msg_desc_from(self, stock_code: str, var: ObservationVariable) -> str:
        if var:
            model = ObservedStockS1Model.from_observation_variable(var)
            desc = f"[{model.concept_name} {model.concept_position}]"
            if model.is_t_limit:
                desc += "[T字线]"
            return desc
    
    def update_trade_data(self):
        with StrategySession() as session:
            # 获取所有相关的ObservationVariable记录
            results = self.strategy.query_observation_data(session=session, include_observation_variable=True, filters=[
                StrategyObservationEntry.trade_date == self.trade_date
            ])
            
            limit_data_source = LimitDataSource(self.trade_date)
            for entry, var in results:
                daily_data = get_stock_daily_data_for_day(stock_code=entry.stock_code, trade_date=self.trade_date)
                if not daily_data:
                    entry.trade_date = TradeCalendar.get_next_trade_date(self.trade_date)
                    session.merge(entry)
                status = '未封板'
                if entry.stock_code in limit_data_source.limit_up_map:
                    status = '强势板' if is_strong_stock_base_limit_data(limit_data_source.limit_up_map[entry.stock_code]) else '弱势板'
                elif entry.stock_code in limit_data_source.limit_down_map:
                    status = '跌停板'
                elif entry.stock_code in limit_data_source.limit_failed_map:
                    status = '炸板'
                # 添加日志输出
                logging.info(f"Updating {entry.stock_code} status to {status} {entry}")
                
                # 显式更新数据库记录
                var.variables['buy_date_status'] = status
                flag_modified(var, "variables")
                session.add(var)
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                logging.error(f"Failed to commit status updates: {str(e)}")
                raise

    def trade_did_end(self):
        self.analysis_observed_stocks()
        self.update_trade_data()

    def handle_buy_stock(self, stock_code: str, minute_data, trade_time: str):
        """
        处理分钟数据的回调函数。

        参数:
            stock_code: str，股票代码。
            minute_data: dict，分钟数据。
            trade_time: str，当前分钟时间。
        """
        logging.info(f"{trade_time} {stock_code} handle_buy_stock")
        buy_info_record_map = self.buy_info_record_map
        observer_buy_info: Optional[BuyInfoRecord] = buy_info_record_map[stock_code]
        
        observed_stock, _ = self.observe_stocks_to_buy.get(stock_code)
        observed_var_mode = self.buy_var_model_map.get(stock_code)
        stock_name =observed_stock.stock_name
        
        open_price = minute_data["open"]
        close_price = minute_data["close"]
        avg_price = self.stock_data_map[stock_code].avg_price
        current_time: datetime.datetime = minute_data["bob"]
        pre_close = self.pre_stock_daily_map[stock_code].close

        curent_open_rate = (open_price/pre_close - 1)*100
        # 初始化策略类型
        if observer_buy_info.tactics_type < 0:
            if open_price > pre_close:
                observer_buy_info.tactics_type = 0
                observer_buy_info.buy_threshold = max(open_price, pre_close * strategy_params.min_buy_threshold)
                observer_buy_info.stop_observe_price = pre_close * 0.98
            else:
                observer_buy_info.tactics_type = 1
                observer_buy_info.buy_threshold = pre_close *strategy_params.min_buy_threshold
                observer_buy_info.stop_observe_price = pre_close * (1 + strategy_params.buy_in_observe_dead_pch / 100)

            if observed_var_mode.is_t_limit and curent_open_rate >= strategy_params.next_date_open_rate_limit:
                logging.info(f"{trade_time} {stock_name} {stock_code} 停止监听原因: 高开幅度 {curent_open_rate} 大于 {strategy_params.next_date_open_rate_limit}, 策略类型 {observer_buy_info.tactics_type}")
                observer_buy_info.stop_buy = True
                self.stop_subscribe_buy_stock(stock_code)

            logging.info(f"{trade_time} {stock_code} 买入策略 {observer_buy_info.tactics_type}")
        else:
            # 策略执行逻辑
            if observed_var_mode.concept_position == '龙一' and curent_open_rate < strategy_params.buy_in_observe_dead_pch:
                logging.info(f"{observed_var_mode.concept_name} {stock_name} 龙一跌幅 {curent_open_rate}, 停止同板块监听")
                for other_stock_code, _ in self.buy_var_model_map.items():
                    other_var_mode = self.buy_var_model_map[other_stock_code]
                    if other_var_mode.concept_name == observed_var_mode.concept_name:
                        other_info = buy_info_record_map[other_stock_code]
                        other_info.stop_buy = True
                        self.stop_subscribe_buy_stock(other_stock_code)
            if observed_var_mode.is_t_limit:
                if close_price < observer_buy_info.stop_observe_price:
                    logging.info(f"{trade_time} {stock_name}  {stock_code} 停止监听原因: 低于停止监听线 {observer_buy_info.stop_observe_price}, 策略类型 {observer_buy_info.tactics_type}")
                    observer_buy_info.stop_buy = True
                    self.stop_subscribe_buy_stock(stock_code)

                if close_price >= observer_buy_info.buy_threshold and close_price >= avg_price:
                    buy_time = current_time
                    buy_price = close_price

                    observer_buy_info.stop_buy = True
                    self.buy_stock(stock_code, buy_price, buy_time)
                    logging.info(f"{self.trade_date} {stock_name} {trade_time} 触发买入线 {observer_buy_info.buy_threshold} 实际买入价格 {buy_price}")

                    # 停止监听同板块其他股票
                    for other_stock_code, (other_stock, _) in self.observe_stocks_to_buy.items():
                        other_var_mode = self.buy_var_model_map[other_stock_code]
                        if other_var_mode.concept_name == observed_var_mode.concept_name and other_stock_code != stock_code:
                            buy_info_record_map[other_stock_code].stop_buy = True
                            logging.info(f"{self.trade_date} {other_stock.stock_name} {trade_time} 停止监听原因: 已买入同板块股票")
                            self.stop_subscribe_buy_stock(other_stock_code,  SubscribeStopType.STOPTYPE_CANCEL)

        if trade_time > "09:40":
            logging.info(f"{trade_time} 超出买入策略执行时间范围，停止监听 {stock_name}")
            observer_buy_info.stop_buy = True
            self.stop_subscribe_buy_stock(stock_code)
            return

    def handle_sell_stock(self, stock_code: str, minute_data, trade_time: str):
        logging.info(f"{trade_time} {stock_code} handle_sell_stock")
        sell_info_record_map = self.sell_info_record_map
        observer_sell_info: Optional[SellInfoRecord] = sell_info_record_map[stock_code]
        sell_data = cast(SellInfoRecordDataYugiS1, observer_sell_info.other_data)
        
        open = minute_data['open']
        close = minute_data['close']
        avg = self.stock_data_map[stock_code].avg_price
        high = minute_data['high']
        low = minute_data['low']
        current_time = minute_data['bob']
        pre_close = self.pre_stock_daily_map[stock_code].close

        if observer_sell_info.tactics_type < 0:
            # 指定卖出策略
            pre_stock_daily_data: Optional[StockDailyData] = self.pre_stock_daily_map[stock_code]
            open_rate = (open / pre_close - 1)*100 

            if open_rate <= strategy_params.limit_down_pch:
                sell_data.strong_level = -2
                observer_sell_info.tactics_type = SellStrategyType.SellLossStopHighThanAvgPrice
            elif open_rate <= -4:
                sell_data.strong_level = -1
                observer_sell_info.tactics_type = SellStrategyType.SellLossStopLowerThanAvgPrice | SellStrategyType.SellLossStopAvgPriceGoLow | SellStrategyType.SellLossStopNeedBuyStock
            elif (pre_stock_daily_data.high / pre_stock_daily_data.pre_close - 1)*100 > strategy_params.limit_up_pch:
                if pre_stock_daily_data.close < pre_stock_daily_data.high:
                    logging.info(f"{stock_code} 买入日炸板。当日卖出")
                    # 炸板
                    sell_data.strong_level = -1
                    observer_sell_info.tactics_type = SellStrategyType.SellLossStopLowerThanAvgPrice | SellStrategyType.SellLossStopAvgPriceGoLow | SellStrategyType.SellLossStopNeedBuyStock
                elif pre_stock_daily_data.close == pre_stock_daily_data.high:
                    # 涨停板
                    sell_data.strong_level = 1
                    observer_sell_info.tactics_type = SellStrategyType.SellProfitStopLowerThanDeadLine |  SellStrategyType.SellProfitStopLowerThanAvgPrice
                    if stock_code in self.pre_limit_data_source.limit_up_map and open_rate < 0:
                        pre_limit_data = self.pre_limit_data_source.limit_up_map[stock_code]
                        if self.is_strong_limit_stock(pre_limit_data):
                            observer_sell_info.tactics_type = observer_sell_info.tactics_type | SellStrategyType.SellLossStopNeedBuyStock | SellStrategyType.SellLossStopLowerThanAvgPrice | SellStrategyType.SellLossStopAvgPriceGoLow
            elif (pre_stock_daily_data.high / pre_stock_daily_data.pre_close - 1)*100 <= strategy_params.limit_up_pch:
                # 未封板
                sell_data.strong_level = -1
                observer_sell_info.tactics_type = SellStrategyType.SellLossStopLowerThanAvgPrice | SellStrategyType.SellLossStopAvgPriceGoLow | SellStrategyType.SellLossStopNeedBuyStock
            
            logging.info(f"{trade_time} {stock_code} 卖出策略 {observer_sell_info.tactics_type}")
            
        logging.debug(f"卖出回调 {trade_time} {stock_code} {open} {close} {avg} {round((high - avg)*100/pre_close, 2)}")
        tactics_type = observer_sell_info.tactics_type
        if trade_time > '09:31':
            if (tactics_type & SellStrategyType.SellLossStopLowerThanAvgPrice) and open - avg < 0 and close - avg < 0:
                sell_reason = f'基于策略{SellStrategyType.SellLossStopLowerThanAvgPrice} 且低于分时黄线'
                logging.info(f"卖出参数 pre_avg_price={sell_data.pre_avg_price}")
                self.sell_stock(stock_code, close, current_time, sell_reason)
                return
            if (tactics_type & SellStrategyType.SellLossStopAvgPriceGoLow) and sell_data.pre_avg_price != None and avg - sell_data.pre_avg_price < -0.01:
                sell_reason = f'基于策略{SellStrategyType.SellLossStopAvgPriceGoLow} 且分时黄线下行'
                logging.info(f" 卖出参数 pre_avg_price={sell_data.pre_avg_price} avg={avg}")
                self.sell_stock(stock_code, open, current_time, sell_reason)
                return
            if (tactics_type & SellStrategyType.SellProfitStopLowerThanAvgPrice):
                if open < avg:
                    if sell_data.under_water_minutes == 0:
                        sell_data.under_water_avg_price_start = avg
                    sell_data.under_water_minutes += 1
                elif open > avg:
                    sell_data.under_water_minutes = 0
                if sell_data.under_water_minutes > strategy_params.under_water_stop_minutes or (sell_data.under_water_avg_price_start and ( avg/sell_data.under_water_avg_price_start-1)*100 < strategy_params.under_water_avg_price_pch_limit):
                    sell_reason = f'基于水下时长{sell_data.under_water_minutes}分钟'
                    logging.info(f"卖出参数 avg={avg} under_water_avg_price_start={sell_data.under_water_avg_price_start} pre_close={pre_close}")
                    self.sell_stock(stock_code, open, current_time, sell_reason)
                    return
            if (tactics_type & SellStrategyType.SellProfitStopLowerThanDeadLine) and (open / pre_close - 1) * 100 < strategy_params.profit_stop:
                sell_reason = f'基于{strategy_params.profit_stop}%止盈'
                logging.info(f"卖出参数 水下时长{sell_data.under_water_minutes}分钟 avg={avg} under_water_avg_price_start={sell_data.under_water_avg_price_start}")
                self.sell_stock(stock_code, open, current_time, sell_reason)
                return
        if trade_time != "09:30":
            if (tactics_type & SellStrategyType.SellLossStopHighThanAvgPrice) and (high - avg)*100/pre_close >= strategy_params.sold_out_pch:
                sell_reason = f'基于分时偏离平均价格 {strategy_params.sold_out_pch}%'
                self.sell_stock(stock_code, close, current_time, sell_reason)
                return
        if trade_time >= "14:54":
            current_pch = (open / pre_close - 1) * 100
            if current_pch < strategy_params.limit_up_pch and current_pch > strategy_params.limit_down_pch:
                sell_reason = f'基于14:54  后未涨停也并未跌停'
                self.sell_stock(stock_code, open, current_time, sell_reason)
                return
            
        first_limit_time = sell_data.first_limit_time
        if not first_limit_time and high == low and (high / pre_close - 1) * 100 > strategy_params.limit_up_pch:
            sell_data.first_limit_time = trade_time
            logging.info(f"首次涨停 {stock_code} {self.trade_date} {first_limit_time}")
            if sell_data.first_limit_time > strategy_params.high_limit_stock_first_time:
                sell_reason = f'基于高位板{strategy_params.high_limit_stock_first_time}后涨停'
                self.sell_stock(stock_code, close, current_time, sell_reason)
                return
        sell_data.pre_avg_price = avg
    
    def buy_stock(self, stock_code, buy_price, buy_time):
        if self.sell_info_record_map:
            for sell_stock_code, observer_sell_info in self.sell_info_record_map.items():
                if (observer_sell_info.tactics_type & SellStrategyType.SellLossStopNeedBuyStock):
                    if not observer_sell_info.is_sell:
                        sell_reason = '基于先卖后买'
                        self.sell_stock(sell_stock_code, 0, buy_time, sell_reason)
        super().buy_stock(stock_code, buy_price, buy_time)

    def analysis_observed_stocks(self):
        next_date = TradeCalendar.get_next_trade_date(self.trade_date)
        self.strategy.clexrar_records(next_date)
        with Session() as session:
            trade_date = self.trade_date
            market_data_today =  session.query(MarketDailyData).filter(MarketDailyData.trade_date.in_([trade_date])).first()
            market_data_sentiment = market_data_today.sentiment_index
            logging.info(f"正在分析{trade_date} 股票, 当日市场情绪指数 {market_data_sentiment}")

            # 提前退出条件
            if market_data_sentiment > strategy_params.cold_sentiment_line:
                return
            

            first_date_concept_to_stocks = {}
            first_limit_count = 0
            second_limit_count = 0
            stock_code_to_limit_result = {}  # 新字典用于存储股票代码到LimitUpTushare的映射

            # 初始化 LimitDataSource
            limit_data_source = LimitDataSource(trade_date)

            # 使用 limit_up_map 和 limit_failed_map 构造 stock_code_to_limit_result
            stock_code_to_limit_result = {
                **limit_data_source.limit_up_map,  # 涨停数据
                **limit_data_source.limit_failed_map,  # 炸板数据
            }

            attribution_results = session.query(LimitDailyAttribution).filter(
                LimitDailyAttribution.trade_date == trade_date
            ).all()

            for attribution in attribution_results:
                if attribution.concept_name == '其它':
                    continue
                stock_code = attribution.stock_code

                if attribution.concept_name not in first_date_concept_to_stocks:
                    first_date_concept_to_stocks[attribution.concept_name] = {
                        "weak_stocks": [],
                        "t_stocks": [],
                        "limit_up_stocks": [],
                        "first_stock": None,
                        "second_stock": None,
                        "recent_highest_stock": None,
                    }

                stock_code = attribution.stock_code
                if stock_code not in stock_code_to_limit_result:
                    continue
                limit_data = stock_code_to_limit_result[stock_code]

                if limit_data.continuous_limit_up_count > first_limit_count:
                    second_limit_count = first_limit_count
                    first_limit_count = limit_data.continuous_limit_up_count
                    
                elif limit_data.continuous_limit_up_count > second_limit_count:
                    second_limit_count = limit_data.continuous_limit_up_count

                # 填充涨停股票池
                if limit_data.limit_status == "U":
                    first_date_concept_to_stocks[attribution.concept_name]["limit_up_stocks"].append(limit_data)
                
                # 填充 weak_stocks
                first_date_daily_data: Optional[StockDailyData] = limit_data_source.get_daily_data(stock_code)
                # weak_stocks = first_date_concept_to_stocks[attribution.concept_name]["weak_stocks"]
                t_stocks = first_date_concept_to_stocks[attribution.concept_name]["t_stocks"]
                stock_is_t_limit = first_date_daily_data.is_t_limit()
                logging.info(f"{first_date_daily_data.ts_code} T字板判定{stock_is_t_limit}")
                

                if stock_is_t_limit:
                    amplitude_limit_condition = ((first_date_daily_data.high - first_date_daily_data.low)*100/first_date_daily_data.pre_close < strategy_params.first_date_amplitude)
                    if limit_data.up_stat:
                        boards, days_ago = map(int, limit_data.up_stat.split('/'))
                    previous_daily_source = StockDailyDataSource(first_date_daily_data.ts_code, trade_date, strategy_params.continuous_one_limit_count)
                    logging.info(f"{first_date_daily_data.ts_code} 振幅判定{amplitude_limit_condition}")
                    if amplitude_limit_condition and not previous_daily_source.is_one_limit():
                        if stock_is_t_limit:
                            t_stocks.append(limit_data)
                        # elif not stock_is_strong_limit:
                        #     avg_price_pch = (first_date_daily_data.amount * 1000 /first_date_daily_data.vol)/first_date_daily_data.pre_close - 100
                        #     if avg_price_pch > 6:
                        #         weak_stocks.append(limit_data)
                
                # 填充 first_stock 和 second_stock
                first_stock = first_date_concept_to_stocks[attribution.concept_name]["first_stock"]
                second_stock = first_date_concept_to_stocks[attribution.concept_name]["second_stock"]
                first_stock_code = first_stock.stock_code if first_stock else None
                strong_stock_code = limit_data_source.compare_stock_strength(first_stock_code, limit_data.stock_code)
                if strong_stock_code != first_stock_code:
                    logging.info(f"{trade_date} {attribution.concept_name} 龙一判定 {limit_data.stock_name} > {first_stock.stock_name if first_stock else None}")
                    first_date_concept_to_stocks[attribution.concept_name]["first_stock"] = limit_data
                    if first_stock:
                        logging.info(f"{trade_date} {attribution.concept_name} 龙二判定 {first_stock.stock_name} > {second_stock.stock_name if second_stock else None}")
                        first_date_concept_to_stocks[attribution.concept_name]["second_stock"] = first_stock
                    
                else:
                    second_stock_code = second_stock.stock_code if second_stock else None
                    strong_stock_code = limit_data_source.compare_stock_strength(second_stock_code, limit_data.stock_code)
                    if strong_stock_code != second_stock_code:
                        logging.info(f"{trade_date} {attribution.concept_name} 龙二判定 {limit_data.stock_name} > {second_stock.stock_name if second_stock else None}")
                        first_date_concept_to_stocks[attribution.concept_name]["second_stock"] = limit_data

                recent_highest_stock = first_date_concept_to_stocks[attribution.concept_name]["recent_highest_stock"]
                recent_highest_stock_code = recent_highest_stock.stock_code if recent_highest_stock else None
                strong_stock_code = limit_data_source.compare_stock_recent_height(recent_highest_stock_code, limit_data.stock_code)
                if strong_stock_code and strong_stock_code != recent_highest_stock_code:
                    logging.info(f"{trade_date} {attribution.concept_name} 近期高度股票判定 {limit_data.stock_name} > {recent_highest_stock.stock_name if recent_highest_stock else None}")
                    first_date_concept_to_stocks[attribution.concept_name]["recent_highest_stock"] = limit_data

            for concept_name, stocks_map in first_date_concept_to_stocks.items():
                logging.info(f"{trade_date} {concept_name} 策略弱势条件判定")
                # 新逻辑：移除弱势股票不包含龙一、龙二股票池中任意股票的板块
                # weak_stocks = stocks_map["weak_stocks"]
                t_stocks = stocks_map["t_stocks"]
                limit_up_stocks = stocks_map["limit_up_stocks"]
                first_stock: Optional[LimitUpTushare] = stocks_map["first_stock"]
                second_stock: Optional[LimitUpTushare] = stocks_map["second_stock"]
                recent_highest_stock: Optional[LimitUpTushare] = stocks_map["recent_highest_stock"]

                logging.info(f"板块 {concept_name}, 涨停股票池数量: {len(limit_up_stocks)}")
                logging.info(f"{trade_date} {concept_name} T字板 {[stock.stock_name for stock in t_stocks]}")
                logging.info(f"{trade_date} 板块{concept_name} 龙一: {first_stock.stock_name}({first_stock.stock_code}) 空间 {first_stock.up_stat} 连板数 {first_stock.continuous_limit_up_count}")
                if second_stock:
                    logging.info(f"{trade_date} 板块{concept_name} 龙二 {second_stock.stock_name}({second_stock.stock_code}) 空间 {second_stock.up_stat}  连板数 {second_stock.continuous_limit_up_count}")
                    
                first_stock.concept_position = "龙一"
                if second_stock:
                    second_stock.concept_position = "龙二"

                dragon_stocks = set()
                dragon_stocks.add(first_stock)
                if second_stock:
                    dragon_stocks.add(second_stock)

                for stock in dragon_stocks:
                    stock_limit_info: Optional[LimitUpTushare] = stock

                    stock.recent_high = limit_data_source.get_pct_chg(stock_limit_info.stock_code)
                    boards, days_ago = map(int, recent_highest_stock.up_stat.split('/'))
                    stock.avg_recent_high = stock.recent_high / days_ago if days_ago > 0 else stock.recent_high

                if first_stock.continuous_limit_up_count < strategy_params.observe_min_limit_count:
                    dragon_stocks.remove(first_stock)

                if second_stock and (second_stock.continuous_limit_up_count == first_stock.continuous_limit_up_count or second_stock.continuous_limit_up_count < strategy_params.observe_min_limit_count):
                    dragon_stocks.remove(second_stock)

                recent_highest_stock.recent_high = limit_data_source.get_pct_chg(recent_highest_stock.stock_code)
                boards, days_ago = map(int, recent_highest_stock.up_stat.split('/'))
                recent_highest_stock.avg_recent_high = round(recent_highest_stock.recent_high / days_ago, 2) if days_ago > 0 else recent_highest_stock.recent_high
                logging.info(f"{trade_date} 板块{concept_name} 最高涨幅股 {recent_highest_stock.stock_name}({recent_highest_stock.stock_code})  近期涨幅 {recent_highest_stock.recent_high} 平均涨幅 {recent_highest_stock.avg_recent_high}")
                second_recent_high = second_stock.recent_high if second_stock else 0
                if recent_highest_stock.avg_recent_high >= strategy_params.avg_recent_high_limit and recent_highest_stock.recent_high > max(strategy_params.recent_high_limit, second_recent_high):
                    if (recent_highest_stock != first_stock and recent_highest_stock != second_stock) or recent_highest_stock.continuous_limit_up_count < strategy_params.observe_min_limit_count:
                        recent_highest_stock.concept_position = "近期涨幅股"
                        dragon_stocks.add(recent_highest_stock)


                if len(t_stocks) < 1:
                    logging.info(f"移除板块 {concept_name}, 因T字板数量不足: {len(t_stocks)}")
                    continue
                    
                concept_observed_pool = set()
                for stock in dragon_stocks:
                    stock_limit_info: Optional[LimitUpTushare] = stock
                    if is_10cm_stock(stock_limit_info.stock_code) and stock_limit_info in t_stocks:
                        if len(limit_up_stocks) > strategy_params.concept_effect_line:
                            concept_observed_pool.add(stock)
                        elif stock.continuous_limit_up_count >= max(second_limit_count,4):
                            concept_observed_pool.add(stock)
                        elif stock == recent_highest_stock and stock.avg_recent_high >= strategy_params.avg_recent_high_limit and stock.recent_high >= max(strategy_params.recent_high_limit, second_recent_high):
                            concept_observed_pool.add(stock)
                if concept_observed_pool:
                    concept_observed_pool.add(first_stock)

                if not concept_observed_pool:
                    continue
                
                for stock in concept_observed_pool:
                    stock_limit_info: Optional[LimitUpTushare] = stock
                    daily_position=None
                    if stock_limit_info.continuous_limit_up_count > second_limit_count:
                        daily_position = "最高空间板"
                    elif stock_limit_info.continuous_limit_up_count == second_limit_count:
                        daily_position = "次高空间板"
                    is_t_limit = (stock_limit_info in t_stocks)
                    observed_model = ObservedStockS1Model(
                        concept_name=concept_name,
                        concept_position=stock_limit_info.concept_position,
                        daily_position=daily_position,
                        up_stat=stock_limit_info.up_stat,
                        amount=stock_limit_info.amount,
                        turnover_ratio=stock_limit_info.turnover_ratio,
                        continuous_limit_up_count=stock_limit_info.continuous_limit_up_count,
                        recent_high_pch=stock_limit_info.recent_high,
                        recent_high_avg=stock_limit_info.avg_recent_high,
                        is_t_limit=is_t_limit
                    )
                    self.add_observation_entry_with_variable(next_date, stock_limit_info.stock_code, stock_limit_info.stock_name, variables=observed_model.model_dump())
        
    def is_strong_limit_stock(self, limit_info: LimitUpTushare):
        if is_strong_stock_base_limit_data(limit_info):
            return True
        else:
            return is_strong_limit_up_base_minute_data(limit_info.stock_code, limit_info.trade_date, limit_info.close,  limit_info.first_time, limit_info.last_time, 5)

class StrategyYugiS1(Strategy):
    def strategy_name(self) -> str:
        return "Yugi_s1"
    
    def get_observation_model(self) -> ObservationVarMode:
        return ObservedStockS1Model
    
    def analyze_performance_datas(self, records: List[Tuple[TradeRecord, StrategyObservationEntry, ObservationVariable]]) -> Dict[str, Any]:
        success_trades = [record[2].variables["buy_date_status"] for record in records 
                            if record[2].variables["buy_date_status"] in ('强势板', '弱势板', '涨停板')]
        success_rate = len(success_trades) / len(records) if len(records) > 0 else 0
        return {
            "策略买入封板股票数": f"{len(success_trades)}",
            "买入当天封板成功率": f"{success_rate:.2%}"
        }
    
    def __init__(self, task_cls: StrategyTask = StrategyTaskYugiS1, params: Type['StrategyParams'] = StrategyParamsYugiS1(), mode: StrategyTaskMode = StrategyTaskMode.MODE_BACKTEST_LOCAL):
        super().__init__(task_cls, params, mode)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("用法: python market_chart.py <开始日期(yyyyMMdd)> <结束日期(yyyyMMdd)>")
        sys.exit(1)
    start_date = sys.argv[1]
    end_date = sys.argv[2]

    strategy = StrategyYugiS1()
    # strategy.publish(clear=True)
    strategy.local_simulation(start_date, end_date)
    strategy.analyze_strategy_performance(start_date, end_date)
    strategy.export_trade_records_to_excel(start_date, end_date)

   