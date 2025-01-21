# coding: utf-8
import logging
import datetime
from a_trade.limit_attribution import LimitDailyAttribution
from a_trade.stocks_daily_data import StockDailyData, StockDailyDataSource,get_stock_daily_data_for_day
from a_trade.trade_calendar import TradeCalendar
from a_trade.limit_up_data_tushare import LimitUpTushare, is_strong_stock_base_limit_data, LimitDataSource
from a_trade.stock_minute_data import is_strong_limit_up_base_minute_data, get_minute_data
from a_trade.db_base import Session, Base, engine
from typing import Optional, Dict, Callable
from a_trade.xlsx_file_manager import XLSXFileManager
from a_trade.market_analysis import MarketDailyData
import a_trade.settings
from a_trade.trade_utils import is_10cm_stock
from a_trade.wechat_bot import WechatBot
from a_trade.strategy_task import StrategyTask, StrategyTaskMode, Strategy, TradeRecord, ObservedStockPool, SubscribeStopType
from a_trade.settings import get_project_path

from enum import IntFlag
from sqlalchemy import Column, String, Float, Integer, func, Boolean,DateTime, and_ 

class SellStrategyType(IntFlag):
    SellLossStopLowerThanAvgPrice = 1         # 股价低于平均成交价格线
    SellLossStopAvgPriceGoLow = 2             # 平均成交价格线走低
    SellLossStopHighThanAvgPrice = 4          # 股价高于平均成交价格线一定幅度
    SellProfitStopLowerThanAvgPrice = 8       # 股价低于于平均成交价格线一定幅度止盈
    SellProfitStopLowerThanDeadLine = 16      # 涨停/跌停，第二天卖出
    SellLossStopNeedBuyStock = 32             # 可以先卖后买，腾出仓位

class ObservedStockPoolS1(ObservedStockPool):
    __tablename__ = 'observed_stocks_s1'
    concept_name = Column(String, primary_key=True)
    concept_position = Column(String, nullable=False)
    daily_position = Column(String)
    continuous_limit_up_count = Column(Integer, nullable=False)    # 连续板数
    up_stat = Column(String, nullable=False)
    amount = Column(Float)
    amount_rate = Column(Float)
    recent_high_pch = Column(Float, nullable=False)
    recent_high_avg = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    is_t_limit = Column(Boolean, nullable=False)
    buy_in = Column(Boolean, nullable=False)

    @classmethod
    def daily_report(cls, trade_date):
        with Session() as session:
            t_stocks = session.query(ObservedStockPoolS1).filter(
                ObservedStockPoolS1.trade_date == trade_date,
                ObservedStockPoolS1.is_t_limit == True
            ).all()

            if t_stocks:
                content = f"今日冰点，明天重点观测T字板 {','.join(list(set([stock_info.stock_name for stock_info in t_stocks])))}"
            else:
                content = "今天没有可观测的冰点T字板策略股票池"

            final_msg = (
                f"【Yugi_s1策略通知】\n"
                f" {content} "
            )
            WechatBot.send_text_msg(final_msg)

class TradeRecordS1(TradeRecord):
    __tablename__ = 'trade_record_s1'

class BuyInfoRecord():
    def __init__(self):
        self.stop_buy = False
        self.buy_threshold = None
        self.tactics_type = -1
        self.stop_observe_price = None

class SellInfoRecord():
     def __init__(self):
        self.first_limit_time = None
        self.is_sell = False
        self.tactics_type = None
        self.strong_level = 0
        self.pre_avg_price = None
        self.under_water_minutes = 0
        self.under_water_avg_price_start = None
        self.pre_close = None

Base.metadata.create_all(engine)

# 冰点线定义，高于这条线，不做弱转强
cold_sentiment_line = 40

# 板块效应下限，达到这个数量的板块不参与弱转强
concept_effect_line = 2

# 板块效应连板数下限，低于这个数量不纳入观察池
observe_min_limit_count = 2

# 非连续涨停板龙头股日均涨幅下限，影响买入股票池
avg_recent_high_limit = 6

# 非连续涨停板龙头股总涨幅下限，影响买入股票池
recent_high_limit = 50

# 弱板开板时间下限定义(分钟)，影响买入股票池
weak_limit_open_time = 10

# 弱板股票/板块龙一 第二天纳入观察池幅度下限，影响买入策略
buy_in_observe_dead_pch = -6

# 弱板股票第一天振幅上限，影响买入策略
first_date_amplitude = 15

# T字板开盘幅度下限。影响买入策略
t_limit_open_condition = 7

# 第二天高开幅度上限, 影响买入策略
next_date_open_rate_limit = 5.9

# 烂板前连续一字板数量上限。达到这个上限，不参与弱转强买入。影响买入策略
continuous_one_limit_count = 3

# 涨停幅度下限，影响卖出策略。
limit_up_pch = 9.8

# 跌停幅度下限，影响卖出策略。
limit_down_pch = -9.8

# 前一天未吃到大面，第二天盘中分时高于黄线幅度触发卖出。影响卖出策略。3-4降低胜率，提升盈亏比
sold_out_pch = 4

# 止盈线
profit_stop = -6

# 连续水下时长，止盈策略
under_water_stop_minutes = 40

# 水下平均成交价跌幅上限
under_water_avg_price_pch_limit = -2

# 高位板首次涨停时间卖出线，晚于这个时间涨停。板上卖出。影响卖出策略。
high_limit_stock_first_time = '14:30'

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
        self.pre_trade_date = TradeCalendar.get_previous_trade_date(trade_date)

        self.observe_stocks_to_buy: Dict[str, ObservedStockPoolS1] = {}
        self.buy_info_record_map: Dict[str, BuyInfoRecord] = {}

        self.observe_stocks_to_sell: Dict[str, TradeRecordS1] = {}
        self.sell_info_record_map: Dict[str, SellInfoRecord] = {}
        self.pre_stock_daily_map: Dict[str, StockDailyData] = {}

        self.prepare_observed_pool()

    def prepare_observed_pool(self):
        """
        准备观察池股票，并将结果写入类变量。
        """
        with Session() as session:
            observed_stock_pool_to_buy = session.query(ObservedStockPoolS1).filter(
                ObservedStockPoolS1.trade_date == self.pre_trade_date
            ).all()
            target_time = datetime.datetime.strptime(self.trade_date, '%Y%m%d')
            one_month_ago = target_time - datetime.timedelta(days=30)
            observed_stock_pool_to_sold = session.query(TradeRecordS1).filter(
                TradeRecordS1.sold_out_price == None,
                TradeRecordS1.buy_time >= one_month_ago,
                TradeRecordS1.buy_time < target_time
            ).all()
            
            self.observe_stocks_to_buy = {
                stock.stock_code: stock for stock in observed_stock_pool_to_buy
            }
            self.observe_stocks_to_sell = {
                stock.stock_code: stock for stock in observed_stock_pool_to_sold
            }
            self.buy_info_record_map = {stock_code: BuyInfoRecord() for stock_code in self.observe_stocks_to_buy.keys()}
            self.sell_info_record_map = {stock_code: SellInfoRecord() for stock_code in self.observe_stocks_to_sell.keys()}
            pre_stock_daily_results = session.query(StockDailyData).filter(
                StockDailyData.ts_code.in_(self.sell_info_record_map.keys()),
                StockDailyData.trade_date == self.pre_trade_date
            ).all()
            self.pre_limit_data_source = LimitDataSource(self.pre_trade_date)
            for pre_daily_data in pre_stock_daily_results:
                observer_sell_info: Optional[SellInfoRecord] = self.sell_info_record_map[pre_daily_data.ts_code]
                observer_sell_info.pre_close = pre_daily_data.close
                self.pre_stock_daily_map[pre_daily_data.ts_code] = pre_daily_data
        
            logging.info(f"{self.trade_date} 买入观察池股票准备完成，共 {len(self.observe_stocks_to_buy)} 只股票  {self.observe_stocks_to_buy.keys()}")
            logging.info(f"{self.trade_date} 持仓观察池股票准备完成，共 {len(self.observe_stocks_to_sell)} 只股票 {self.observe_stocks_to_sell.keys()}")
            logging.info(f"开始微信通知 {self._enable_send_msg()}")
            if self._enable_send_msg():
                # 1) 整理“买入观察池”列表
                buy_stocks = []
                for stock_code, stock in self.observe_stocks_to_buy.items():
                    # 也可以添加更多信息，比如 (stock_code, stock_name, concept_name...)
                    buy_stocks.append((stock_code, stock.stock_name, stock.concept_name, stock.concept_position,stock.is_t_limit))

                # 2) 整理“卖出(持仓)观察池”列表
                sell_stocks = []
                for stock_code, record in self.observe_stocks_to_sell.items():
                    sell_stocks.append((record.stock_code, record.stock_name))

                # 3) 调用微信机器人发送观察池消息
                WechatBot.send_observe_pool_msg(self.trade_date, buy_stocks, sell_stocks)
    
    def stop_subscribe_buy_stock(self, stock_code, stop_type: SubscribeStopType = SubscribeStopType.STOPTYPE_CANCEL):
        if stop_type != SubscribeStopType.STOPTYPE_SELL:
            if stock_code not in self.observe_stocks_to_sell:
                super().stop_subscribe_buy_stock(stock_code, stop_type)
        else:
            super().stop_subscribe_buy_stock(stock_code, stop_type)

    def trade_did_end(self):
        with Session() as session:
            today = datetime.datetime.strptime(self.trade_date, "%Y%m%d").date()
            records = session.query(TradeRecordS1).filter(
                func.date(TradeRecordS1.buy_time) == today
            ).all()
            limit_data_source = LimitDataSource(self.trade_date)
            for record in records:
                if record.stock_code in limit_data_source.limit_up_map:
                    if is_strong_stock_base_limit_data(limit_data_source.limit_up_map[record.stock_code]):
                        record.buy_date_status = '强势板'
                    else:
                        record.buy_date_status = '弱势板'
                elif record.stock_code in limit_data_source.limit_down_map:
                    record.buy_date_status = '跌停板'
                elif record.stock_code in limit_data_source.limit_failed_map:
                    record.buy_date_status = '炸板'
                else:
                    record.buy_date_status = '未封板'
            session.commit()

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
        if stock_code not in buy_info_record_map:
            return
        observer_buy_info = buy_info_record_map[stock_code]
        if observer_buy_info.stop_buy:
            return
        
        observed_stock = self.observe_stocks_to_buy.get(stock_code)
        
        open_price = minute_data["open"]
        close_price = minute_data["close"]
        avg_price = self.stock_data_map[stock_code].avg_price
        current_time: datetime.datetime = minute_data["bob"]

        curent_open_rate = (open_price/observed_stock.close - 1)*100
        logging.info(f"{trade_time} {stock_code} handle_buy_stock data ready {current_time}")
        # 初始化策略类型
        if observer_buy_info.tactics_type < 0:
            if open_price > observed_stock.close:
                observer_buy_info.tactics_type = 0
                observer_buy_info.buy_threshold = max(open_price, observed_stock.close * 1.01)
                observer_buy_info.stop_observe_price = observed_stock.close * 0.98
            else:
                observer_buy_info.tactics_type = 1
                observer_buy_info.buy_threshold = observed_stock.close *1.01
                observer_buy_info.stop_observe_price = observed_stock.close * (1 + buy_in_observe_dead_pch / 100)

            if observed_stock.is_t_limit and curent_open_rate >= next_date_open_rate_limit:
                logging.info(f"{trade_time} {observed_stock.stock_name} {observed_stock.stock_code} 停止监听原因: 高开幅度 {curent_open_rate} 大于 {next_date_open_rate_limit}, 策略类型 {observer_buy_info.tactics_type}")
                observer_buy_info.stop_buy = True
                self.stop_subscribe_buy_stock(stock_code)

            logging.info(f"{trade_time} {stock_code} 卖出策略 {observer_buy_info.tactics_type}")
        else:
            # 策略执行逻辑
            if observed_stock.is_t_limit:
                if close_price < observer_buy_info.stop_observe_price:
                    logging.info(f"{trade_time} {observed_stock.stock_name}  {observed_stock.stock_code} 停止监听原因: 低于停止监听线 {observer_buy_info.stop_observe_price}, 策略类型 {observer_buy_info.tactics_type}")
                    observer_buy_info.stop_buy = True
                    self.stop_subscribe_buy_stock(stock_code)

                if close_price >= observer_buy_info.buy_threshold and close_price >= avg_price:
                    buy_time = current_time
                    buy_price = close_price

                    observer_buy_info.stop_buy = True
                    self.buy_stock(observed_stock.stock_code, observed_stock.stock_name, buy_price, buy_time)
                    logging.info(f"{self.trade_date} {observed_stock.stock_name} {trade_time} 触发买入线 {observer_buy_info.buy_threshold} 实际买入价格 {buy_price}")

                    # 停止监听同板块其他股票
                    for other_stock_code, other_stock in self.observe_stocks_to_buy.items():
                        if other_stock.concept_name == observed_stock.concept_name and other_stock_code != observed_stock.stock_code:
                            buy_info_record_map[other_stock_code].stop_buy = True
                            logging.info(f"{self.trade_date} {other_stock.stock_name} {trade_time} 停止监听原因: 已买入同板块股票")
                            self.stop_subscribe_buy_stock(other_stock_code,  SubscribeStopType.STOPTYPE_CANCEL)

        if observed_stock.concept_position == '龙一' and curent_open_rate < buy_in_observe_dead_pch:
            logging.info(f"{observed_stock.concept_name} {observed_stock.stock_name} 龙一跌幅 {curent_open_rate}, 停止同板块监听")
            for other_stock in self.observe_stocks_to_buy.values():
                if other_stock.concept_name == observed_stock.concept_name:
                    other_info = buy_info_record_map[other_stock.stock_code]
                    other_info.stop_buy = True
                    self.stop_subscribe_buy_stock(other_stock.stock_code)

        if trade_time > "09:40":
            logging.info(f"{trade_time} 超出买入策略执行时间范围，停止监听 {observed_stock.stock_name}")
            observer_buy_info.stop_buy = True
            self.stop_subscribe_buy_stock(stock_code)
            return

    def handle_sell_stock(self, stock_code: str, minute_data, trade_time: str):
        logging.info(f"{trade_time} {stock_code} handle_sell_stock start")
        sell_info_record_map = self.sell_info_record_map
        if stock_code not in sell_info_record_map:
            return
        observer_sell_info: Optional[SellInfoRecord] = sell_info_record_map[stock_code]
        if observer_sell_info.is_sell:
            return
        
        pre_stock_daily_map = self.pre_stock_daily_map

        open = minute_data['open']
        close = minute_data['close']
        avg = self.stock_data_map[stock_code].avg_price
        high = minute_data['high']
        low = minute_data['low']
        current_time = minute_data['bob']
        logging.info(f"{trade_time} {stock_code} handle_sell_stock data ready {current_time} {avg}")
        if observer_sell_info.tactics_type == None:
            # 指定卖出策略
            pre_stock_daily_data: Optional[StockDailyData] = pre_stock_daily_map[stock_code]
            open_rate = (open / observer_sell_info.pre_close - 1)*100 

            if open_rate <= limit_down_pch:
                observer_sell_info.strong_level = -2
                observer_sell_info.tactics_type = SellStrategyType.SellLossStopHighThanAvgPrice
            elif open_rate <= -4:
                observer_sell_info.strong_level = -1
                observer_sell_info.tactics_type = SellStrategyType.SellLossStopLowerThanAvgPrice | SellStrategyType.SellLossStopAvgPriceGoLow | SellStrategyType.SellLossStopNeedBuyStock
            elif (pre_stock_daily_data.high / pre_stock_daily_data.pre_close - 1)*100>limit_up_pch:
                if pre_stock_daily_data.close < pre_stock_daily_data.high:
                    logging.info(f"{stock_code} 买入日炸板。当日卖出")
                    # 炸板
                    observer_sell_info.strong_level = -1
                    observer_sell_info.tactics_type = SellStrategyType.SellLossStopLowerThanAvgPrice | SellStrategyType.SellLossStopAvgPriceGoLow | SellStrategyType.SellLossStopNeedBuyStock
                elif pre_stock_daily_data.close == pre_stock_daily_data.high:
                    # 涨停板
                    observer_sell_info.strong_level = 1
                    observer_sell_info.tactics_type = SellStrategyType.SellProfitStopLowerThanDeadLine |  SellStrategyType.SellProfitStopLowerThanAvgPrice
                    if stock_code in self.pre_limit_data_source.limit_up_map and open_rate < 0:
                        pre_limit_data = self.pre_limit_data_source.limit_up_map[stock_code]
                        if self.is_strong_limit_stock(pre_limit_data):
                            observer_sell_info.tactics_type = observer_sell_info.tactics_type | SellStrategyType.SellLossStopNeedBuyStock | SellStrategyType.SellLossStopLowerThanAvgPrice | SellStrategyType.SellLossStopAvgPriceGoLow
            elif (pre_stock_daily_data.high / pre_stock_daily_data.pre_close - 1)*100<=limit_up_pch:
                # 未封板
                observer_sell_info.strong_level = -1
                observer_sell_info.tactics_type = SellStrategyType.SellLossStopLowerThanAvgPrice | SellStrategyType.SellLossStopAvgPriceGoLow | SellStrategyType.SellLossStopNeedBuyStock
            
            logging.info(f"{trade_time} {stock_code} 卖出策略 {observer_sell_info.tactics_type}")
            
        logging.debug(f"卖出回调 {trade_time} {stock_code} {open} {close} {avg} {round((high - avg)*100/observer_sell_info.pre_close, 2)}")
        tactics_type = observer_sell_info.tactics_type
        if trade_time > '09:31':
            if (tactics_type & SellStrategyType.SellLossStopLowerThanAvgPrice) and open - avg < 0 and close - avg < 0:
                sell_reason = f'基于策略{SellStrategyType.SellLossStopLowerThanAvgPrice} 且低于分时黄线'
                logging.info(f"卖出 {stock_code} {trade_time} {close} {avg} {observer_sell_info.pre_avg_price} {sell_reason}")
                self.sell_stock(stock_code, close, current_time, observer_sell_info, sell_reason)
                return
            if (tactics_type & SellStrategyType.SellLossStopAvgPriceGoLow) and observer_sell_info.pre_avg_price != None and avg - observer_sell_info.pre_avg_price < -0.01:
                sell_reason = f'基于策略{SellStrategyType.SellLossStopAvgPriceGoLow} 且分时黄线下行'
                logging.info(f"卖出 {stock_code} {trade_time} {open} {avg} {observer_sell_info.pre_avg_price} {sell_reason}")
                self.sell_stock(stock_code, open, current_time, observer_sell_info, sell_reason)
                return
            if (tactics_type & SellStrategyType.SellProfitStopLowerThanAvgPrice):
                if open < avg:
                    if observer_sell_info.under_water_minutes == 0:
                        observer_sell_info.under_water_avg_price_start = avg
                    observer_sell_info.under_water_minutes += 1
                elif open > avg:
                    observer_sell_info.under_water_minutes = 0
                if observer_sell_info.under_water_minutes > under_water_stop_minutes or (observer_sell_info.under_water_avg_price_start and ( avg/observer_sell_info.under_water_avg_price_start-1)*100 < under_water_avg_price_pch_limit):
                    sell_reason = f'基于水下时长{observer_sell_info.under_water_minutes}分钟'
                    logging.info(f"卖出 {stock_code} {trade_time} {open} {current_time} {sell_reason} {avg} {observer_sell_info.under_water_avg_price_start} {observer_sell_info.pre_close}")
                    self.sell_stock(stock_code, open, current_time, observer_sell_info, sell_reason)
                    return
            if (tactics_type & SellStrategyType.SellProfitStopLowerThanDeadLine) and (open / observer_sell_info.pre_close - 1) * 100 < profit_stop:
                sell_reason = f'基于{profit_stop}%止盈'
                logging.info(f"卖出 {stock_code} {trade_time} {open} {current_time} {sell_reason}，水下时长{observer_sell_info.under_water_minutes}分钟 {avg} {observer_sell_info.under_water_avg_price_start}")
                self.sell_stock(stock_code, open, current_time, observer_sell_info, sell_reason)
                return
        if trade_time != "09:30":
            if (tactics_type & SellStrategyType.SellLossStopHighThanAvgPrice) and (high - avg)*100/observer_sell_info.pre_close >= sold_out_pch:
                sell_reason = f'基于分时偏离平均价格 {sold_out_pch}%'
                logging.info(f"卖出 {stock_code} {trade_time} {close} {trade_time} {sell_reason}")
                self.sell_stock(stock_code, close, current_time, observer_sell_info, sell_reason)
                return
        if trade_time >= "14:54":
            current_pch = (open / observer_sell_info.pre_close - 1) * 100
            if current_pch < limit_up_pch and current_pch > limit_down_pch:
                sell_reason = f'基于14:54  后未涨停也并未跌停'
                logging.info(f"卖出 {stock_code} {trade_time} {open} {self.trade_date} {sell_reason}")
                self.sell_stock(stock_code, open, current_time, observer_sell_info, sell_reason)
                return
            
        first_limit_time = observer_sell_info.first_limit_time
        if not first_limit_time and high == low and (high / observer_sell_info.pre_close - 1) * 100 > limit_up_pch:
            observer_sell_info.first_limit_time = trade_time
            logging.info(f"首次涨停 {stock_code} {self.trade_date} {first_limit_time}")
            if observer_sell_info.first_limit_time > high_limit_stock_first_time:
                sell_reason = f'基于高位板{high_limit_stock_first_time}后涨停'
                logging.info(f"卖出 {stock_code} {close} {trade_time} {sell_reason}")
                self.sell_stock(stock_code, close, current_time, observer_sell_info, sell_reason)
                return
        observer_sell_info.pre_avg_price = avg
    
    def buy_stock(self, stock_code, stock_name, buy_price, buy_time):
        self.stop_subscribe_buy_stock(stock_code, SubscribeStopType.STOPTYPE_BUY)
        if self.sell_info_record_map:
            for sell_stock_code, observer_sell_info in self.sell_info_record_map.items():
                if (observer_sell_info.tactics_type & SellStrategyType.SellLossStopNeedBuyStock):
                    if not observer_sell_info.is_sell:
                        sell_reason = '基于先卖后买'
                        logging.info(f"卖出 {sell_stock_code} {buy_time} {sell_reason}")
                        self.sell_stock(sell_stock_code, 0, buy_time, observer_sell_info, sell_reason)
                        
        if self.run_mode != StrategyTaskMode.MODE_BACKTEST_LOCAL:
            self.buy_callback(stock_code, stock_name, buy_price, buy_time)
        if self._enable_send_msg():
            WechatBot.send_buy_stock_msg(stock_code, stock_name, buy_price, buy_time)
        with Session() as session:
            new_record = TradeRecordS1(
                stock_code=stock_code,
                stock_name=stock_name,
                buy_price=buy_price,
                buy_time=buy_time,
                buy_date_status="未封板",  
                sold_out_price=None,  # 卖出逻辑未触发，默认为 None
                sold_out_time=None,
                profit_rate=None
            )
            session.add(new_record)
            session.commit()
            logging.info(f"记录买入交易: {new_record}")
            
    def sell_stock(self, stock_code: str, sold_price: float, trade_time: datetime.datetime, observer_sell_info: SellInfoRecord, sell_reason: str):
        if self.run_mode != StrategyTaskMode.MODE_BACKTEST_LOCAL:
            self.sell_callback(stock_code, sold_price, trade_time)
        
        if self._enable_send_msg():
            WechatBot.send_sold_stock_msg(stock_code, sold_price, trade_time.strftime("%Y-%m-%d %H:%M:%S"),sell_reason)

        observer_sell_info.is_sell = True
        self.stop_subscribe_buy_stock(stock_code, SubscribeStopType.STOPTYPE_SELL)
        with Session() as session:
            target_time = datetime.datetime.strptime(self.trade_date, '%Y%m%d')
            one_month_ago = target_time - datetime.timedelta(days=30)
            record = session.query(TradeRecordS1).filter(
                TradeRecordS1.stock_code == stock_code,
                TradeRecordS1.sold_out_price == None,
                TradeRecordS1.buy_time < target_time,
                TradeRecordS1.buy_time > one_month_ago,
            ).first()
            if record:
                if self.run_mode == StrategyTaskMode.MODE_BACKTEST_LOCAL and sold_price == 0:
                    minutes_data = get_minute_data(stock_code, self.trade_date)
                    for minute in minutes_data:
                        if datetime.datetime.strptime(minute.trade_time, "%Y-%m-%d %H:%M:%S") == trade_time:
                            sold_price = minute.close
                record.sold_out_time = trade_time
                record.sold_out_price = sold_price
                record.profit_rate = round(((sold_price - record.buy_price) / record.buy_price) * 100,2)
                session.commit()

    def is_strong_limit_stock(self, limit_info: LimitUpTushare):
        if is_strong_stock_base_limit_data(limit_info):
            return True
        else:
            return is_strong_limit_up_base_minute_data(limit_info.stock_code, limit_info.trade_date, limit_info.close,  limit_info.first_time, limit_info.last_time, 5)

class StrategyYugiS1(Strategy):
    def __init__(self, start_date, end_date, observe_pool_cls=ObservedStockPoolS1, trade_record_cl=TradeRecordS1, task_cls=StrategyTaskYugiS1):
        super().__init__(start_date, end_date, observe_pool_cls, trade_record_cl, task_cls)

    def analysis_observed_stocks_pool_for_day(self, trade_date):
        with Session() as session:
            market_data_today =  session.query(MarketDailyData).filter(MarketDailyData.trade_date.in_([trade_date])).first()
            market_data_sentiment = market_data_today.sentiment_index
            logging.info(f"正在分析{trade_date} 股票, 当日市场情绪指数 {market_data_sentiment}")

            # 提前退出条件
            if market_data_sentiment > cold_sentiment_line:
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
                    amplitude_limit_condition = ((first_date_daily_data.high - first_date_daily_data.low)*100/first_date_daily_data.pre_close < first_date_amplitude)
                    if limit_data.up_stat:
                        boards, days_ago = map(int, limit_data.up_stat.split('/'))
                    previous_daily_source = StockDailyDataSource(first_date_daily_data.ts_code, trade_date, continuous_one_limit_count)
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

                if first_stock.continuous_limit_up_count < observe_min_limit_count:
                    dragon_stocks.remove(first_stock)

                if second_stock and (second_stock.continuous_limit_up_count == first_stock.continuous_limit_up_count or second_stock.continuous_limit_up_count < observe_min_limit_count):
                    dragon_stocks.remove(second_stock)

                recent_highest_stock.recent_high = limit_data_source.get_pct_chg(recent_highest_stock.stock_code)
                boards, days_ago = map(int, recent_highest_stock.up_stat.split('/'))
                recent_highest_stock.avg_recent_high = round(recent_highest_stock.recent_high / days_ago, 2) if days_ago > 0 else recent_highest_stock.recent_high
                logging.info(f"{trade_date} 板块{concept_name} 最高涨幅股 {recent_highest_stock.stock_name}({recent_highest_stock.stock_code})  近期涨幅 {recent_highest_stock.recent_high} 平均涨幅 {recent_highest_stock.avg_recent_high}")
                second_recent_high = second_stock.recent_high if second_stock else 0
                if recent_highest_stock.avg_recent_high >= avg_recent_high_limit and recent_highest_stock.recent_high > max(recent_high_limit, second_recent_high):
                    if (recent_highest_stock != first_stock and recent_highest_stock != second_stock) or recent_highest_stock.continuous_limit_up_count < observe_min_limit_count:
                        recent_highest_stock.concept_position = "近期涨幅股"
                        dragon_stocks.add(recent_highest_stock)


                if len(t_stocks) < 1:
                    logging.info(f"移除板块 {concept_name}, 因T字板数量不足: {len(t_stocks)}")
                    continue
                    
                concept_observed_pool = set()
                for stock in dragon_stocks:
                    stock_limit_info: Optional[LimitUpTushare] = stock
                    if is_10cm_stock(stock_limit_info.stock_code) and stock_limit_info in t_stocks:
                        if len(limit_up_stocks) > concept_effect_line:
                            concept_observed_pool.add(stock)
                        elif stock.continuous_limit_up_count >= max(second_limit_count,4):
                            concept_observed_pool.add(stock)
                        elif stock == recent_highest_stock and stock.avg_recent_high >= avg_recent_high_limit and stock.recent_high >= max(recent_high_limit, second_recent_high):
                            concept_observed_pool.add(stock)
                if concept_observed_pool:
                    concept_observed_pool.add(first_stock)

                if not concept_observed_pool:
                    continue
                
                for stock in concept_observed_pool:
                    stock_limit_info: Optional[LimitUpTushare] = stock
                    daily_position=None
                    if stock_limit_info.continuous_limit_up_count > second_limit_count:
                        daily_position = "次高空间板"
                    elif stock_limit_info.continuous_limit_up_count == second_limit_count:
                        daily_position = "最高空间板"

                    is_t_limit = (stock_limit_info in t_stocks)
                    new_record = ObservedStockPoolS1(
                        trade_date=trade_date,
                        stock_code=stock_limit_info.stock_code,
                        stock_name=stock_limit_info.stock_name,
                        concept_name=concept_name,
                        concept_position=stock_limit_info.concept_position,
                        daily_position=daily_position,
                        up_stat=stock_limit_info.up_stat,
                        amount=stock_limit_info.amount,
                        amount_rate = stock_limit_info.turnover_ratio,
                        continuous_limit_up_count=stock_limit_info.continuous_limit_up_count,
                        recent_high_pch=stock_limit_info.recent_high,
                        recent_high_avg=stock_limit_info.avg_recent_high,
                        close=stock_limit_info.close,
                        is_t_limit=is_t_limit,
                        buy_in = 0
                    )
                    session.add(new_record)
                session.commit()
            
    def export_trade_records_to_excel(self):
        start_date = self.start_date
        end_date = self.end_date

        trade_record_path = get_project_path() / f"strategy_result/Yugi_s1交易记录{start_date}_{end_date}.xlsx"
        trade_file_manager = XLSXFileManager(trade_record_path, "历史交易记录", ["板块名称", "股票代码", "股票名称", "烂板当日成交额占比(%)", "连板数", "购买日", "购买价格", "买入当天是否封板", "买入日情绪指标", "买入日开盘(%)", "卖出日", "卖出价格", "盈利(%)", "close", "open", "high", "low", "avg", "close", "open", "high", "low", "avg"], [2, 5], True)
        with Session() as session:
            # 将输入日期格式化为 datetime 对象
            start_time = datetime.datetime.strptime(start_date, "%Y%m%d")
            end_time = datetime.datetime.strptime(end_date, "%Y%m%d") + datetime.timedelta(hours=23, minutes=59, seconds=59)

            # 查询 TradeRecordS1 数据
            trade_records = session.query(TradeRecordS1).filter(
                TradeRecordS1.buy_time.between(start_time, end_time),
                TradeRecordS1.sold_out_price != None
            ).all()

            results = []
            for trade_record in trade_records:
                # 获取 buy_time 的前一个交易日
                buy_date_str = trade_record.buy_time.strftime("%Y%m%d")
                previous_trade_date = TradeCalendar.get_previous_trade_date(buy_date_str)
                # 查询 ObservedStockPoolS1 匹配数据
                observed_record = session.query(ObservedStockPoolS1).filter(
                    ObservedStockPoolS1.stock_code == trade_record.stock_code,
                    ObservedStockPoolS1.trade_date == previous_trade_date
                ).first()

                # 将匹配的记录加入结果
                results.append((trade_record, observed_record))
            
            for trade_record, observed_record in results:
                # 处理 observed_record 为 None 的情况
                concept_name = observed_record.concept_name if observed_record else "未知概念"
                continuous_limit_up_count = observed_record.continuous_limit_up_count if observed_record else 0
                buy_date_str = trade_record.buy_time.strftime("%Y%m%d")
                sentiment = session.query(MarketDailyData).filter(
                    MarketDailyData.trade_date == buy_date_str
                ).first().sentiment_index
                daily_data = session.query(StockDailyData).filter(
                    StockDailyData.trade_date == buy_date_str,
                    StockDailyData.ts_code == trade_record.stock_code
                ).first()
                open_rate = round((daily_data.open / daily_data.pre_close - 1)*100,2)
                
                observed_date = observed_record.trade_date
                previous_daily_source = StockDailyDataSource(observed_record.stock_code, observed_date, continuous_one_limit_count)
                observed_daily_data = get_stock_daily_data_for_day(observed_record.stock_code, observed_date)
                vol_rate = observed_daily_data.vol / previous_daily_source.get_highest_vol()
                print(f"location: {observed_daily_data.vol} {previous_daily_source.get_highest_vol()} {vol_rate}")
                excel_record = [
                    concept_name,
                    trade_record.stock_code,
                    trade_record.stock_name,
                    vol_rate,
                    continuous_limit_up_count,
                    trade_record.buy_time.strftime("%Y-%m-%d %H:%M:%S"),
                    trade_record.buy_price,
                    trade_record.buy_date_status,
                    sentiment,
                    open_rate,
                    trade_record.sold_out_time.strftime("%Y-%m-%d %H:%M:%S"),
                    trade_record.sold_out_price,
                    trade_record.profit_rate
                ]
                
                trade_file_manager.insert_and_save(excel_record)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("用法: python market_chart.py <开始日期(yyyyMMdd)> <结束日期(yyyyMMdd)>")
        sys.exit(1)
    start_date = sys.argv[1]
    end_date = sys.argv[2]

    strategy = StrategyYugiS1(start_date, end_date)
    # strategy.analysis_observed_stocks_pool()

    strategy.start_trade_by_strategy()
    strategy.analyze_strategy_performance()
    # strategy.export_trade_records_to_excel()

   