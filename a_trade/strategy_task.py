# coding: utf-8
from enum import Enum
from abc import ABC, abstractmethod
from typing import Callable, Optional, Dict, Type
import logging
import datetime
from dataclasses import dataclass
from sqlalchemy import and_, func, Column, String, Float, DateTime, Integer, Boolean

from a_trade.db_base import Session
from a_trade.stock_subscription import StockSubscriptionBus
from a_trade.trade_calendar import TradeCalendar
from a_trade.db_base import Base
from a_trade.wechat_bot import WechatBot

class ObservedStockPool(Base):
    __abstract__ = True
    trade_date = Column(String, primary_key=True)
    stock_code = Column(String, primary_key=True)
    stock_name = Column(String, nullable=False)

class TradeRecord(Base):
    __abstract__ = True

    stock_code = Column(String, primary_key=True)
    stock_name  = Column(String, nullable=False)
    buy_time = Column(DateTime, primary_key=True)
    buy_price = Column(Float, nullable=False)
    buy_date_status = Column(String)
    sold_out_price = Column(Float)
    sold_out_time = Column(DateTime)
    profit_rate=Column(Float)

    def __repr__(self):
        return f"<{self.__class__}(stock_code={self.stock_code}, stock_name={self.stock_name}, buy_price={self.buy_price}, sold_out_price={self.sold_out_price})>"

@dataclass
class StockMinuteModel:
    total_amount: float = 0.0
    total_volume: float = 0.0
    avg_price: float = 0.0

class StrategyTaskMode(Enum):
    MODE_BACKTEST_LOCAL = 0
    MODE_BACKTEST_EMQUANT = 1
    MODE_LIVE_EMQUANT = 2

class SubscribeStopType(Enum):
    STOPTYPE_CANCEL = 0
    STOPTYPE_BUY = 1
    STOPTYPE_SELL = 2

class StrategyTask(ABC):
    """
    通用策略任务基类，封装策略执行的通用流程。
    """

    def __init__(self, trade_date: str, mode: StrategyTaskMode = StrategyTaskMode.MODE_BACKTEST_LOCAL,
                 buy_callback: Optional[Callable] = None, sell_callback: Optional[Callable] = None,
                 subscribe_callback: Optional[Callable] = None, unsubscribe_callback: Optional[Callable] = None,
                 enable_send_msg: bool = False):
        self.trade_date = trade_date
        self.run_mode = mode
        self.enable_send_msg = enable_send_msg

        self.buy_callback = buy_callback
        self.sell_callback = sell_callback
        self.subscribe_callback = subscribe_callback
        self.unsubscribe_callback = unsubscribe_callback

        self.observe_stocks_to_buy = {}
        self.observe_stocks_to_sell = {}

        # 使用单一字典存储累计数据
        self.stock_data_map: Dict[str, StockMinuteModel] = {}

        if self.run_mode != StrategyTaskMode.MODE_BACKTEST_LOCAL:
            assert buy_callback is not None, "buy_callback 不能为空"
            assert sell_callback is not None, "sell_callback 不能为空"
            assert subscribe_callback is not None, "subscribe_callback 不能为空"
            assert unsubscribe_callback is not None, "unsubscribe_callback 不能为空"
        self.subscription = StockSubscriptionBus(trade_date)

    def _enable_send_msg(self) -> bool:
        if self.run_mode == StrategyTaskMode.MODE_LIVE_EMQUANT:
            return True
        return self.enable_send_msg

    def will_subscribe_stocks(self):
        """
        订阅股票数据并设置回调
        """
        pass
        if not self.observe_stocks_to_buy and not self.observe_stocks_to_sell:
            logging.info(f"{self.trade_date} 没有需要处理的股票")
            return

        observed_codes = list(set(list(self.observe_stocks_to_buy.keys()) + list(self.observe_stocks_to_sell.keys())))
        self.subscription.subscribe(observed_codes, self.on_minute_data)
        if self.run_mode != StrategyTaskMode.MODE_BACKTEST_LOCAL:
            self.subscribe_callback(observed_codes)
       

    def stop_subscribe_buy_stock(self, stock_code: str, stop_type: SubscribeStopType = SubscribeStopType.STOPTYPE_CANCEL):
        success = self.subscription.unsubscribe(stock_code)
        if success and self.run_mode != StrategyTaskMode.MODE_BACKTEST_LOCAL:
            self.unsubscribe_callback(stock_code)
        print(f"location: {stop_type}")
        if success and stop_type == SubscribeStopType.STOPTYPE_CANCEL and self._enable_send_msg():
            WechatBot.send_stop_subscribe_msg(stock_code)

    @abstractmethod
    def prepare_observed_pool(self):
        """
        准备今日观察池，具体逻辑由子类实现。
        """
        pass

    def on_minute_data(self, stock_code: str, minute_data, trade_time: str):
        """
        处理分钟数据的回调函数，实现平均累计成交价的计算并保存。子类复写需要调用父类方法
        
        参数:
            stock_code: str，股票代码。
            minute_data: dict，包含 'amount' 和 'volume' 等键。
            trade_time: str，当前分钟时间。
        """
        current_time : datetime.datetime = minute_data['bob']
        if current_time.strftime("%Y%m%d") != self.trade_date:
            logging.warning(f"收到不属于{self.trade_date}的分时数据")
            return
        try:
            # 获取当前分钟的成交额和成交量
            current_amount = minute_data['amount']
            current_volume = minute_data['volume']

            # 初始化累积数据如果股票代码尚未存在
            if stock_code not in self.stock_data_map:
                self.stock_data_map[stock_code] = StockMinuteModel()

            stock_data = self.stock_data_map[stock_code]

            # 更新累计成交额和累计成交量
            stock_data.total_amount += current_amount
            stock_data.total_volume += current_volume

            # 防止除以零
            if stock_data.total_volume == 0:
                logging.warning(f"累计成交量为零，无法计算平均成交价，股票代码: {stock_code}")
                stock_data.avg_price = 0.0
            else:
                # 计算平均累计成交价，保留两位小数
                stock_data.avg_price = round(stock_data.total_amount / stock_data.total_volume, 2)

                logging.debug(f"股票代码: {stock_code}, 时间: {trade_time}, "
                              f"累计成交额: {stock_data.total_amount}, "
                              f"累计成交量: {stock_data.total_volume}, "
                              f"平均成交价: {stock_data.avg_price}")

        except Exception as e:
            logging.error(f"处理分钟数据时发生错误，股票代码 {stock_code}, 日期 {self.trade_date}: {e}")
        
        # 调用策略评估方法
        self.handle_sell_stock(stock_code, minute_data, trade_time)
        self.handle_buy_stock(stock_code, minute_data, trade_time)
    
    @abstractmethod
    def handle_sell_stock(self, stock_code: str, minute_data, trade_time: str):
        """
        策略按分时卖出钩子方法，子类可以根据需要重写此方法以实现具体的买卖逻辑。
        
        参数:
            stock_code: str，股票代码。
            trade_time: str，当前分钟时间。
        """
        pass

    @abstractmethod
    def handle_buy_stock(self, stock_code: str, minute_data, trade_time: str):
        """
        策略按分时卖出钩子方法，子类可以根据需要重写此方法以实现具体的买卖逻辑。
        
        参数:
            stock_code: str，股票代码。
            trade_time: str，当前分钟时间。
        """
        pass

    @abstractmethod
    def buy_stock(self, stock_code, stock_name, buy_price, buy_time):
        """
        执行买入操作，具体逻辑由子类实现。
        """
        pass

    @abstractmethod
    def sell_stock(self, stock_code: str, sold_price: float, trade_time: datetime.datetime):
        """
        执行卖出操作，具体逻辑由子类实现。
        """
        pass

    @abstractmethod
    def trade_did_end(self):
        """
        当日交易结束操作，具体逻辑由子类实现。
        """

    def start_local_trade(self):
        if self.run_mode == StrategyTaskMode.MODE_BACKTEST_LOCAL:
            self.subscription.start_trade()


class Strategy(ABC):

    def __init__(self, start_date: str, end_date: str, observe_pool_cls: Type['ObservedStockPool'], trade_record_cl: Type['TradeRecord'], task_cls: Type['StrategyTask']):
        super().__init__()
        self.start_date = start_date
        self.end_date = end_date
        self.observe_pool_cls = observe_pool_cls
        self.trade_record_cls = trade_record_cl
        self.task_cls = task_cls

    def analysis_observed_stocks_pool(self):
        self.clear_stock_pool()
        TradeCalendar.iterate_trade_days(self.start_date, self.end_date, self.analysis_observed_stocks_pool_for_day)
    
    def start_trade_by_strategy(self):
        self.clear_trade_record()
        TradeCalendar.iterate_trade_days(self.start_date, self.end_date, self.start_trade_by_strategy_for_day)

    def clear_stock_pool(self):
        """
        删除指定日期范围内的记录。

        :param session: SQLAlchemy 的数据库会话对象
        :param start_date: 开始日期（格式如 '20250101'）
        :param end_date: 结束日期（格式如 '20250131'）
        """
        if self.observe_pool_cls:
            with Session() as session:
                try:
                    # 构建删除条件
                    delete_condition = and_(
                        self.observe_pool_cls.trade_date >= self.start_date,
                        self.observe_pool_cls.trade_date <= self.end_date
                    )
                    # 执行删除操作
                    deleted_count = session.query(self.observe_pool_cls).filter(delete_condition).delete(synchronize_session=False)
                    session.commit()
                    print(f"成功删除 {deleted_count} 条记录。")
                except Exception as e:
                    session.rollback()
                    print(f"删除记录时发生错误: {e}")

    def clear_trade_record(self):
        """
        清理 LimitDailyAttribution 表中指定日期范围内的数据
        """
        if self.trade_record_cls:
            try:
                # 将 YYYYMMDD 格式转换为 datetime 对象，并设置时间范围
                start_time = datetime.datetime.strptime(self.start_date, "%Y%m%d").replace(hour=9, minute=30, second=0)
                end_time = datetime.datetime.strptime(self.end_date, "%Y%m%d").replace(hour=15, minute=0, second=0)
            except ValueError as e:
                raise ValueError(f"日期格式错误，请使用 'YYYYMMDD' 格式: {e}")

            with Session() as session:
                try:
                    # 查询并删除指定时间范围内的数据
                    rows_deleted = session.query(self.trade_record_cls).filter(
                        and_(
                            self.trade_record_cls.buy_time >= start_time.strftime("%Y-%m-%d %H:%M:%S"),
                            self.trade_record_cls.buy_time <= end_time.strftime("%Y-%m-%d %H:%M:%S")
                        )
                    ).delete(synchronize_session=False)
                    observed_stock_pool = session.query(self.observe_pool_cls).filter(
                        self.observe_pool_cls.trade_date.between(self.start_date, self.end_date)
                    ).all()
                    for observed_stock_info in observed_stock_pool:
                        observed_stock_info.buy_in = False
                    session.commit()
                    print(f"成功删除了 {rows_deleted} 条记录")
                except Exception as e:
                    session.rollback()
                    raise RuntimeError(f"清理记录时发生错误: {e}")

    def start_trade_by_strategy_for_day(self, trade_date):
        if self.task_cls:
            task = self.task_cls(trade_date)
            task.will_subscribe_stocks()
            task.start_local_trade()
            task.trade_did_end()
   
    def analyze_strategy_performance(self):
        """
        收益分析
        """
        if not self.trade_record_cls:
            return
        start_date = self.start_date
        end_date = self.end_date
        with Session() as session:
            # 将输入日期格式化为 datetime 对象
            start_time = datetime.datetime.strptime(start_date, "%Y%m%d")
            end_time = datetime.datetime.strptime(end_date, "%Y%m%d") + datetime.timedelta(hours=23, minutes=59, seconds=59)

            # 按 buy_time 日期部分聚合并统计交易天数
            trade_days_t = session.query(
                func.count(func.distinct(func.date(self.trade_record_cls.buy_time)))
            ).filter(
                self.trade_record_cls.buy_time.between(start_time, end_time)
            ).scalar()

            total_days = (end_time - start_time).days+1  # 计算总天数
            if total_days <= 0:
                raise ValueError("起始日期必须早于结束日期")
            total_years = total_days / 365  # 转换为年

            # 从数据库中获取交易记录
            records = session.query(self.trade_record_cls).filter(
                self.trade_record_cls.buy_time.between(start_time, end_time),
                self.trade_record_cls.sold_out_price != None
            ).all()
            if not records:
                print(f"没有交易数据")
                return

            # 总交易数
            total_count = len(records)

            # 分类盈利和亏损交易
            positive_trades = [record.profit_rate for record in records if record.profit_rate > 0]
            negative_trades = [record.profit_rate for record in records if record.profit_rate < 0]

            success_trades = [record.buy_date_status for record in records 
                            if record.buy_date_status in ('强势板', '弱势板', '涨停板')]
            success_rate = len(success_trades) / total_count if total_count > 0 else 0

            # 计算胜率
            positive_count = len(positive_trades)
            win_rate = positive_count / total_count if total_count > 0 else 0

            # 计算平均盈利和平均亏损
            total_profit = sum(positive_trades)
            total_loss = sum(negative_trades)

            # 计算盈亏比
            profit_loss_ratio = total_profit / abs(total_loss) if total_loss != 0 else 0

            # 计算累计资金（复利效应）
            initial_funds = 50000
            final_funds = initial_funds
            for record in records:
                profit_rate = record.profit_rate / 100  # 将百分比转化为小数
                final_funds *= (1 + profit_rate)

            # 计算年化率
            cagr = ((final_funds / initial_funds) ** (1 / total_years) - 1) if total_years > 0 else None
            avg_buy_day = round(total_days / trade_days_t, 1) if trade_days_t > 0 else 0

            # 打印结果
            print(f"出手天数: {trade_days_t}")
            print(f"策略平均买入次数 {avg_buy_day} 天/次")
            print(f"股票池总数: {total_count}")
            print(f"策略涨跌幅>0的股票数: {positive_count}")
            print(f"策略买入封板股票数: {len(success_trades)}")
            print(f"胜率: {win_rate:.2%}")
            print(f"买入当天封板成功率: {success_rate:.2%}")
            print(f"总盈利: {total_profit}")
            print(f"总亏损: {total_loss}")
            print(f"盈亏比: {profit_loss_ratio:.2f}" if profit_loss_ratio else "无亏损交易，无法计算盈亏比")
            print(f"初始资金: {initial_funds}")
            print(f"考虑复利后的累计资金量: {final_funds:.2f}")
            print(f"年化率: {cagr:.2%}" if cagr else "无法计算年化率")

    @abstractmethod
    def analysis_observed_stocks_pool_for_day(self, trade_date):
        """
        生成每日观察池，用于第二天StrategyTask监听
        """
        pass

    @abstractmethod     
    def export_trade_records_to_excel(self):
        """
        交易数据库导出至excel
        """
        pass