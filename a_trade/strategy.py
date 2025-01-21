# coding: utf-8
import os
import json
from dotenv import load_dotenv
from enum import Enum as PyEnum
from abc import ABC, abstractmethod
from typing import Callable, Optional, Dict, Type, List, Tuple, Union, Any
import logging
from pydantic import BaseModel
import hashlib
import datetime
from dataclasses import dataclass
from sqlalchemy import (
    and_, func, Column, String, DateTime, Integer, DECIMAL, create_engine,
    ForeignKey, UniqueConstraint, ForeignKeyConstraint, Date, Index)
from sqlalchemy.dialects.postgresql import JSONB, ENUM
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError

from a_trade.stock_subscription import StockSubscriptionBus
from a_trade.trade_calendar import TradeCalendar
from a_trade.stock_minute_data import get_minute_data
from a_trade.wechat_bot import WechatBot
from a_trade.settings import get_project_path
from a_trade.xlsx_file_manager import XLSXFileManager
from decimal import Decimal

# 策略数据库配置
load_dotenv()
STRATEGY_DB_USERNAME = os.getenv('STRATEGY_DB_USERNAME')
STRATEGY_DB_PASSWORD = os.getenv('STRATEGY_DB_PASSWORD')
STRATEGY_DB_HOST = os.getenv('STRATEGY_DB_HOST', 'localhost')
STRATEGY_DB_PORT = os.getenv('STRATEGY_DB_PORT', '5432')
STRATEGY_DB_NAME = os.getenv('STRATEGY_DB_NAME')

# 创建数据库 URL
STRATEGY_DB_URL = f'postgresql+pg8000://{STRATEGY_DB_USERNAME}:{STRATEGY_DB_PASSWORD}@{STRATEGY_DB_HOST}:{STRATEGY_DB_PORT}/{STRATEGY_DB_NAME}'
logging.info(f"策略数据库地址: {STRATEGY_DB_URL}")
# PostgreSQL 数据库连接配置
strategy_engine = create_engine(STRATEGY_DB_URL, echo=False)
StrategySession = sessionmaker(bind=strategy_engine)
StrategyBase = declarative_base()
class StrategyTaskMode(PyEnum):
    MODE_BACKTEST_LOCAL = "MODE_BACKTEST_LOCAL"
    MODE_BACKTEST_EMQUANT = "MODE_BACKTEST_EMQUANT"
    MODE_LIVE_EMQUANT = "MODE_LIVE_EMQUANT"

class StrategyInfo(StrategyBase):
    __tablename__ = 'strategy_info'
    strategy_id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String(100), nullable=False, unique=True)
    release_version_id = Column(Integer, nullable=False)
    
    versions = relationship(
        "StrategyVersion",
        back_populates="strategy",
        cascade="all, delete-orphan"
    )
    
    def __repr__(self):
        return f"<StrategyInfo(strategy_id={self.strategy_id}, strategy_name='{self.strategy_name}', release_version_id='{self.release_version_id}')>"

class StrategyVersion(StrategyBase):
    __tablename__ = 'strategy_version'
    strategy_id = Column(Integer, ForeignKey('strategy_info.strategy_id'), primary_key=True)
    version_id = Column(Integer, primary_key=True)
    parameters = Column(JSONB, nullable=False)  # 使用 JSONB 存储策略参数
    version_hash = Column(String(32), nullable=False, unique=True)  # MD5 哈希值，唯一标识版本
    
    # 关系：每个策略版本属于一个策略
    strategy = relationship("StrategyInfo", back_populates="versions")
    
    # 关系：每个策略版本有多个观察条目
    observation_entries = relationship(
        "StrategyObservationEntry",
        back_populates="strategy_version",
        cascade="all, delete-orphan"
    )
    
    def __repr__(self):
        return (f"<StrategyVersion(strategy_id={self.strategy_id}, version_id={self.version_id}, "
                f"version_hash='{self.version_hash}', parameters={self.parameters})>")

class StrategyObservationEntry(StrategyBase):
    __tablename__ = 'strategy_observation_entry'
    entry_id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_id = Column(Integer, nullable=False)
    version_id = Column(Integer, nullable=False)
    trade_date = Column(String(8), nullable=False)
    stock_code = Column(String(20), nullable=False)
    stock_name = Column(String(100), nullable=False)
    
    __table_args__ = (
        ForeignKeyConstraint(
            ['strategy_id', 'version_id'],
            ['strategy_version.strategy_id', 'strategy_version.version_id']
        ),
        UniqueConstraint(
            'strategy_id', 'version_id', 'trade_date', 'stock_code',
            name='uix_strategy_observation_entry'
        )
    )
    
    # 关系：每个观察条目属于一个策略版本
    strategy_version = relationship("StrategyVersion", back_populates="observation_entries")
    
    # 关系：每个观察条目有多个交易记录
    trade_records = relationship(
        "TradeRecord",
        back_populates="observation_entry",
        cascade="all, delete-orphan"
    )
    
    # 关系：每个观察条目有一个对应的 ObservationVariable（单一）
    observation_variable = relationship(
        "ObservationVariable",
        back_populates="observation_entry",
        uselist=False,
        cascade="all, delete-orphan"
    )
    
    def __repr__(self):
        return (f"<StrategyObservationEntry(entry_id={self.entry_id}, strategy_id={self.strategy_id}, "
                f"version_id={self.version_id}, trade_date={self.trade_date}, "
                f"stock_code='{self.stock_code}', stock_name='{self.stock_name}')>")

class TradeRecord(StrategyBase):
    __tablename__ = 'trade_record'
    trade_id = Column(Integer, primary_key=True, autoincrement=True)
    entry_id = Column(Integer, ForeignKey('strategy_observation_entry.entry_id'), nullable=False)
    mode = Column(ENUM(StrategyTaskMode, name='strategy_task_mode'), nullable=False)  # 0: 本地数据库回测, 1: 东财量化历史数据回测, 2: 东财量化实盘监听数据回测
    buy_time = Column(DateTime)
    buy_price = Column(DECIMAL(10, 2))
    sell_time = Column(DateTime)
    sell_price = Column(DECIMAL(10, 2))
    profit_rate = Column(DECIMAL(5, 2))
    
    __table_args__ = (
        UniqueConstraint(
            'entry_id', 'mode',
            name='uix_trade_record_entry_mode'
        ),
    )
    
    # 关系：每个交易记录属于一个观察条目
    observation_entry = relationship("StrategyObservationEntry", back_populates="trade_records")
    
    def __repr__(self):
        return (f"<TradeRecord(trade_id={self.trade_id}, entry_id={self.entry_id}, "
                f"mode={self.mode}, buy_time={self.buy_time}, "
                f"buy_price={self.buy_price}, sell_time={self.sell_time}, "
                f"sell_price={self.sell_price}, profit_rate={self.profit_rate})>")

class ObservationVariable(StrategyBase):
    __tablename__ = 'observation_variable'
    variable_id = Column(Integer, primary_key=True, autoincrement=True)
    entry_id = Column(Integer, ForeignKey('strategy_observation_entry.entry_id'), unique=True, nullable=False)
    variables = Column(JSONB, nullable=False)
    
    __table_args__ = (
        Index(
            'idx_observation_variable_variables',
            'variables',
            postgresql_using='gin'
        ),
    )
    
    observation_entry = relationship("StrategyObservationEntry", back_populates="observation_variable")
    
    def __repr__(self):
        return (f"<ObservationVariable(variable_id={self.variable_id}, entry_id={self.entry_id}, "
                f"variables={self.variables})>")

StrategyBase.metadata.create_all(strategy_engine)

@dataclass
class StockMinuteModel:
    total_amount: float = 0.0
    total_volume: float = 0.0
    avg_price: float = 0.0

class SubscribeStopType(PyEnum):
    STOPTYPE_CANCEL = 0
    STOPTYPE_BUY = 1
    STOPTYPE_SELL = 2

class BuyInfoRecord():
    def __init__(self):
        self.stop_buy = False
        self.tactics_type = -1
        self.buy_threshold = None
        self.stop_observe_price = None
        self.other_data = None

class SellInfoRecord():
     def __init__(self):
        self.is_sell = False
        self.tactics_type = -1
        self.other_data = None

class StrategyParams(BaseModel, ABC):
    def to_md5(self) -> str:
        parameters_json = json.dumps(self.model_dump(), sort_keys=True)
        md5_hash = hashlib.md5(parameters_json.encode('utf-8')).hexdigest()
        return md5_hash
    
class ObservationVarMode(BaseModel):
    @classmethod
    def get_descriptions(cls) -> List[Optional[str]]:
        """
        返回所有字段的 description，顺序与 cls.model_fields 中的 key 顺序一致。
        """
        return [
            cls.model_fields[field].description
            for field in cls.model_fields
        ]
    
    @classmethod
    def from_observation_variable(cls, observation_variable: ObservationVariable) -> 'ObservationVarMode':
        """
        从 ObservationVariable 的 JSONB 数据转化为 ObservationVarMode 子类实例。
        """
        return cls(**observation_variable.variables)

    
class StrategyTask(ABC):
    """
    通用策略任务基类，封装策略每日执行的通用流程。
        - 读取每日监听股票池
        - 监听bar数据，买卖股票
        - 盘后数据整理
    """

    def __init__(self, strategy: 'Strategy', trade_date: str, mode: StrategyTaskMode = StrategyTaskMode.MODE_BACKTEST_LOCAL, enable_send_msg: bool = False):
        self.trade_date = trade_date
        self.run_mode = mode
        self.strategy = strategy

        self.observe_stocks_to_buy = {}
        self.observe_stocks_to_sell = {}

        # 使用单一字典存储累计数据
        self.stock_data_map: Dict[str, StockMinuteModel] = {}
        self.subscription = StockSubscriptionBus(trade_date)

        self.observe_stocks_to_buy: Dict[str, Tuple[StrategyObservationEntry, ObservationVariable]] = {}
        self.buy_info_record_map: Dict[str, BuyInfoRecord] = {}

        self.observe_stocks_to_sell: Dict[str, Tuple[TradeRecord, StrategyObservationEntry]] = {}
        self.sell_info_record_map: Dict[str, SellInfoRecord] = {}
        self.enable_send_msg = enable_send_msg
    
    def prepare_observed_pool(self) -> bool:    
        if self.run_mode != StrategyTaskMode.MODE_BACKTEST_LOCAL:
            assert self.buy_callback is not None, "buy_callback 不能为空"
            assert self.sell_callback is not None, "sell_callback 不能为空"
            assert self.subscribe_callback is not None, "subscribe_callback 不能为空"
            assert self.unsubscribe_callback is not None, "unsubscribe_callback 不能为空"
        is_trade_date = TradeCalendar.is_trade_day(self.trade_date)
        if is_trade_date:
            with StrategySession() as session:
                # 查询当前交易日的策略观察条目
    
                entry_results = self.strategy.query_observation_data(session=session, include_observation_variable=True, filters=[
                    StrategyObservationEntry.trade_date == self.trade_date
                ])
                
                # 将查询结果存入observe_stocks_to_buy字典
                self.observe_stocks_to_buy = {
                    entry.stock_code: (entry, var) for entry, var in entry_results
                }
                
                # 查询未完成卖出的交易记录，并联结观察条目以获取股票代码
                trade_records = self.strategy.query_trade_record_data(session=session, filters=[
                    TradeRecord.sell_price == None,
                    StrategyObservationEntry.trade_date < self.trade_date,
                ])
                
                # 将查询结果存入 observe_stocks_to_sell 字典
                self.observe_stocks_to_sell = {
                    entry.stock_code: (trade_record, entry) for trade_record, entry, _ in trade_records
                }
                
                self.buy_info_record_map = {stock_code: BuyInfoRecord() for stock_code in self.observe_stocks_to_buy.keys()}
                self.sell_info_record_map = {stock_code: SellInfoRecord() for stock_code in self.observe_stocks_to_sell.keys()}

                logging.info(f"{self.trade_date} 买入观察池股票准备完成，共 {len(self.observe_stocks_to_buy)} 只股票  {self.observe_stocks_to_buy.keys()}")
                logging.info(f"{self.trade_date} 持仓观察池股票准备完成，共 {len(self.observe_stocks_to_sell)} 只股票 {self.observe_stocks_to_sell.keys()}")
                logging.info(f"开始微信通知 {self._enable_send_msg()}")

                if self._enable_send_msg():
                    # 1) 整理“买入观察池”列表
                    buy_stocks = []
                    for stock_code, (entry, var) in self.observe_stocks_to_buy.items():
                        entry: StrategyObservationEntry
                        var: ObservationVariable
                        # 也可以添加更多信息，比如 (stock_code, stock_name, concept_name...)
                        buy_stocks.append((stock_code, entry.stock_name, self.msg_desc_from(var)))

                    # 2) 整理“卖出(持仓)观察池”列表
                    sell_stocks = []
                    for stock_code, (_, entry) in self.observe_stocks_to_sell.items():
                        entry: StrategyObservationEntry
                        sell_stocks.append((entry.stock_code, entry.stock_name))

                    # 3) 调用微信机器人发送观察池消息
                    WechatBot.send_observe_pool_msg(self.trade_date, buy_stocks, sell_stocks)

        return is_trade_date
        
    def _enable_send_msg(self) -> bool:
        if self.run_mode == StrategyTaskMode.MODE_LIVE_EMQUANT:
            return True
        return self.enable_send_msg

    def will_subscribe_stocks(self):
        """
        订阅股票数据并设置回调
        """
        if not self.observe_stocks_to_buy and not self.observe_stocks_to_sell:
            logging.info(f"{self.trade_date} 没有需要处理的股票")
            return

        observed_codes = list(set(list(self.observe_stocks_to_buy.keys()) + list(self.observe_stocks_to_sell.keys())))
        self.subscription.subscribe(observed_codes, self.on_minute_data)
        if self.run_mode != StrategyTaskMode.MODE_BACKTEST_LOCAL:
            self.subscribe_callback(observed_codes)

    def stop_subscribe_buy_stock(self, stock_code: str, stop_type: SubscribeStopType = SubscribeStopType.STOPTYPE_CANCEL):
        stop_enable = True
        if stop_type != SubscribeStopType.STOPTYPE_SELL and stock_code in self.observe_stocks_to_sell:
            stop_enable = False
        if stop_enable:
            first_stop = self.subscription.unsubscribe(stock_code)
            if first_stop and self.run_mode != StrategyTaskMode.MODE_BACKTEST_LOCAL:
                self.unsubscribe_callback(stock_code)
            if first_stop and stop_type == SubscribeStopType.STOPTYPE_CANCEL and self._enable_send_msg():
                WechatBot.send_stop_subscribe_msg(stock_code)

    def buy_stock(self, stock_code: str, buy_price: float, buy_time: datetime.datetime):
        buy_price = Decimal(buy_price)
        self.stop_subscribe_buy_stock(stock_code, SubscribeStopType.STOPTYPE_BUY)
        observe_stock, _ = self.observe_stocks_to_buy[stock_code]
        stock_name = observe_stock.stock_name
        if self.run_mode != StrategyTaskMode.MODE_BACKTEST_LOCAL:
            self.buy_callback(stock_code, stock_name, buy_price, buy_time)
        if self._enable_send_msg():
            WechatBot.send_buy_stock_msg(stock_code, stock_name, buy_price, buy_time)
            
        with StrategySession() as session:
            new_record = TradeRecord(
                entry_id=observe_stock.entry_id,
                mode=self.run_mode,
                buy_price=buy_price,
                buy_time=buy_time,
                sell_price=None,
                sell_time=None,
                profit_rate=None,
            )
            session.add(new_record)
            session.commit()
            logging.info(f"记录买入交易: {new_record}")
            
    def sell_stock(self, stock_code: str, sell_price: float, trade_time: datetime.datetime, sell_reason: str=None):
        sell_price = Decimal(sell_price)
        logging.info(f"卖出 {stock_code} {sell_price} {trade_time} 基于 {sell_reason}")
        observer_sell_info = self.sell_info_record_map[stock_code]

        if self.run_mode != StrategyTaskMode.MODE_BACKTEST_LOCAL:
            self.sell_callback(stock_code, sell_price, trade_time)
        
        if self._enable_send_msg():
            WechatBot.send_sold_stock_msg(stock_code, sell_price, trade_time.strftime("%Y-%m-%d %H:%M:%S"),sell_reason)

        observer_sell_info.is_sell = True
        self.stop_subscribe_buy_stock(stock_code, SubscribeStopType.STOPTYPE_SELL)
        with StrategySession() as session:
            target_time = datetime.datetime.strptime(self.trade_date, '%Y%m%d')
            results = self.strategy.query_trade_record_data(session=session, filters=[
                StrategyObservationEntry.stock_code == stock_code,
                TradeRecord.sell_price == None,
                TradeRecord.buy_time < target_time,
            ])
            if len(results) > 0:
                record = results[0][0]
                if self.run_mode == StrategyTaskMode.MODE_BACKTEST_LOCAL and sell_price == 0:
                    minutes_data = get_minute_data(stock_code, self.trade_date, trade_time)
                    for minute in minutes_data:
                        if datetime.datetime.strptime(minute.trade_time, "%Y-%m-%d %H:%M:%S") == trade_time:
                            sell_price = Decimal(minute.close)
                record.sell_time = trade_time
                record.sell_price = sell_price
                record.profit_rate = round(((sell_price - record.buy_price) / record.buy_price) * 100,2)
                session.commit()

    def start_local_trade(self):
        if self.run_mode == StrategyTaskMode.MODE_BACKTEST_LOCAL:
            self.subscription.start_trade()

    def add_observation_entry_with_variable(self, trade_date: str, stock_code: str, stock_name: str, variables: dict = None) -> Optional[StrategyObservationEntry]:
        """
        添加策略观察条目及对应的观察变量到数据库

        参数:
            trade_date: str, 交易日期 (格式: YYYYMMDD)
            stock_code: str, 股票代码
            stock_name: str, 股票名称
            variables: dict, 观察变量字典，默认为空字典

        返回:
            StrategyObservationEntry: 新创建的观察条目对象，如果已存在则返回None
        """
        if not hasattr(self.strategy, 'strategy_id') or not hasattr(self.strategy, 'version_id'):
            raise ValueError("请先创建策略版本")
            
        if variables is None:
            variables = {}

        with StrategySession() as session:
            try:
                # 创建新的观察条目
                new_entry = StrategyObservationEntry(
                    strategy_id=self.strategy.strategy_id,
                    version_id=self.strategy.version_id,
                    trade_date=trade_date,
                    stock_code=stock_code,
                    stock_name=stock_name
                )
                
                # 创建对应的观察变量
                new_variable = ObservationVariable(
                    observation_entry=new_entry,
                    variables=variables
                )
                
                # 添加并提交
                session.add(new_entry)
                session.add(new_variable)
                session.commit()
                logging.info(f"成功添加观察条目及变量: {new_entry}")
                if self.trade_date == trade_date and new_entry.stock_code not in self.observe_stocks_to_buy:
                    self.observe_stocks_to_buy[stock_code] = (new_entry, new_variable)
                return new_entry
            except IntegrityError:
                session.rollback()
                logging.warning(f"观察条目已存在: {stock_code}@{trade_date}")
                return None
            except Exception as e:
                session.rollback()
                logging.error(f"添加观察条目及变量时发生错误: {e}")
                raise
            
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
        if stock_code in self.sell_info_record_map:
            observer_sell_info: Optional[SellInfoRecord] = self.sell_info_record_map[stock_code]
            if not observer_sell_info.is_sell:
                self.handle_sell_stock(stock_code, minute_data, trade_time)

        if stock_code in self.buy_info_record_map:
            observer_buy_info: Optional[BuyInfoRecord] = self.buy_info_record_map[stock_code]
            if not observer_buy_info.stop_buy:
                self.handle_buy_stock(stock_code, minute_data, trade_time)

    def config_callback(self, buy_callback: Optional[Callable] = None, sell_callback: Optional[Callable] = None,
                      subscribe_callback: Optional[Callable] = None, unsubscribe_callback: Optional[Callable] = None,
                 enable_send_msg: bool = False):
        self.buy_callback = buy_callback
        self.sell_callback = sell_callback
        self.subscribe_callback = subscribe_callback
        self.unsubscribe_callback = unsubscribe_callback
        
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
    def msg_desc_from(self, stock_code: str) -> str:
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
    def trade_did_end(self):
        """
        当日交易结束操作，具体逻辑由子类实现。
        """
        pass

    @abstractmethod
    def daily_report(self):
        pass


class Strategy(ABC):
    def __init__(self, task_cls: Type['StrategyTask'], params: Type['StrategyParams'], mode: StrategyTaskMode = StrategyTaskMode.MODE_BACKTEST_LOCAL):
        super().__init__()
        self.run_mode = mode
        self.params = params

        self.task_cls = task_cls
        assert task_cls is not None, "task_cls 不能为空"

        # 获取策略名称
        strategy_name = self.strategy_name()
        if not strategy_name:
            raise ValueError("策略名称不能为空")
            
        # 检查策略信息是否存在
        with StrategySession() as session:
            strategy = session.query(StrategyInfo).filter(
                StrategyInfo.strategy_name == strategy_name
            ).first()
            
            # 如果不存在则插入新记录
            if not strategy:
                strategy = StrategyInfo(
                    strategy_name=strategy_name,
                    release_version_id=0
                )
                session.add(strategy)
                session.commit()
                logging.info(f"已创建新策略记录: {strategy_name}")
            self.strategy_id = strategy.strategy_id
        
        # 计算参数哈希值
        params_md5 = self.params.to_md5()
        
        with StrategySession() as session:
            # 查询是否已存在相同参数版本
            version_exists = session.query(StrategyVersion).filter(
                StrategyVersion.strategy_id == self.strategy_id,
                StrategyVersion.version_hash == params_md5
            ).first()
            
            if not version_exists:
                # 获取当前最大版本号
                max_version = session.query(func.max(StrategyVersion.version_id)).filter(
                    StrategyVersion.strategy_id == strategy.strategy_id
                ).scalar() or 0
                
                # 创建新版本记录
                new_version = StrategyVersion(
                    strategy_id=strategy.strategy_id,
                    version_id=max_version + 1,
                    parameters=self.params.model_dump(),
                    version_hash=params_md5
                )
                session.add(new_version)
                session.commit()
                logging.info(f"已创建新策略版本: {strategy.strategy_name} v{max_version + 1}")
                
                # 设置策略ID和版本ID
                self.version_id = max_version + 1
            else:
                # 如果版本已存在，直接使用现有ID
                self.version_id = version_exists.version_id

    def publish(self, clear=False):
        # 获取策略名称
        strategy_name = self.strategy_name()
        if not strategy_name:
            raise ValueError("策略名称不能为空")
            
        # 计算参数哈希值
        params_md5 = self.params.to_md5()
        
        with StrategySession() as session:
            # 查询策略信息
            strategy = session.query(StrategyInfo).filter(
                StrategyInfo.strategy_name == strategy_name
            ).first()
            
            if not strategy:
                raise ValueError("策略不存在，请先创建策略")
                
            # 查询当前参数对应的版本
            version = session.query(StrategyVersion).filter(
                StrategyVersion.strategy_id == strategy.strategy_id,
                StrategyVersion.version_hash == params_md5
            ).first()
            
            if not version:
                raise ValueError("未找到对应的策略版本，请先创建版本")
                
            release_version_id = version.version_id
            # 如果clear为True，删除非发布版本的所有数据
            if clear:
                # 获取所有需要删除的entry_id
                entry_ids = session.query(StrategyObservationEntry.entry_id).filter(
                    StrategyObservationEntry.strategy_id == strategy.strategy_id,
                    StrategyObservationEntry.version_id != release_version_id
                ).subquery()

                # 批量删除相关表数据，按依赖关系顺序删除
                # 1. 删除交易记录
                session.query(TradeRecord).filter(
                    TradeRecord.entry_id.in_(entry_ids)
                ).delete(synchronize_session=False)
                
                # 2. 删除观察变量
                session.query(ObservationVariable).filter(
                    ObservationVariable.entry_id.in_(entry_ids)
                ).delete(synchronize_session=False)
                
                # 3. 最后删除观察条目
                session.query(StrategyObservationEntry).filter(
                    StrategyObservationEntry.strategy_id == strategy.strategy_id,
                    StrategyObservationEntry.version_id != release_version_id
                ).delete(synchronize_session=False)
                # 4. 删除其他非发布版本的策略版本
                session.query(StrategyVersion).filter(
                    StrategyVersion.strategy_id == strategy.strategy_id,
                    StrategyVersion.version_id != release_version_id
                ).delete(synchronize_session=False)
                logging.info(f"已清理策略 {strategy.strategy_name} 的非发布版本数据")
            # 更新发布版本
            strategy.release_version_id = version.version_id
            session.commit()
            logging.info(f"已发布策略版本: {strategy.strategy_name} v{version.version_id}")

    def generate_daily_task(self, trade_date: str) -> StrategyTask:
        """
        根据交易日期生成策略任务实例
        
        参数:
            trade_date: str, 交易日期
        返回:
            StrategyTask: 策略任务实例
        """
        if not hasattr(self, 'strategy_id') or not hasattr(self, 'version_id'):
            raise ValueError("请先创建策略版本")
            
        if not hasattr(self, 'task_cls'):
            raise ValueError("task_cls未定义")
            
        task = self.task_cls(
            strategy=self,
            trade_date=trade_date)
        return task

    def strategy_simulation_daily_work(self, trade_date):
        task = self.generate_daily_task(trade_date)
        task.prepare_observed_pool()
        task.will_subscribe_stocks()
        task.start_local_trade()
        task.trade_did_end()

    def clear_records(self, start_date: str, end_date: str) -> None:
        """
        清理指定日期范围内的当前策略版本的观察数据

        参数:
            start_date: str, 开始日期 (格式: YYYYMMDD)
            end_date: str, 结束日期 (格式: YYYYMMDD)
        """
        if not hasattr(self, 'strategy_id') or not hasattr(self, 'version_id'):
            raise ValueError("请先创建策略版本")
        logging.info(f"本地运行运行策略{self.strategy_name()} 版本v{self.version_id} 参数{self.params}")
        with StrategySession() as session:
            try:
                # 构建删除条件，限定当前策略版本
                delete_condition = and_(
                    StrategyObservationEntry.trade_date >= start_date,
                    StrategyObservationEntry.trade_date <= end_date,
                    StrategyObservationEntry.strategy_id == self.strategy_id,
                    StrategyObservationEntry.version_id == self.version_id
                )
                
                # 获取要删除的entry_ids
                entries_to_delete = session.query(StrategyObservationEntry).filter(delete_condition).all()
                entry_ids = [entry.entry_id for entry in entries_to_delete]
                
                if entry_ids:
                    # 删除关联的交易记录
                    session.query(TradeRecord).filter(
                        TradeRecord.entry_id.in_(entry_ids),
                        TradeRecord.mode == self.run_mode  # 限定当前mode
                    ).delete(synchronize_session=False)
                    
                    # 删除关联的ObservationVariable
                    session.query(ObservationVariable).filter(
                        ObservationVariable.entry_id.in_(entry_ids)
                    ).delete(synchronize_session=False)
                
                # 删除StrategyObservationEntry
                deleted_count = session.query(StrategyObservationEntry).filter(delete_condition).delete(synchronize_session=False)
                
                session.commit()
                logging.info(f"成功删除 {deleted_count} 条当前策略版本的观察数据、关联变量及交易记录")
            except Exception as e:
                session.rollback()
                logging.error(f"删除策略观察数据时发生错误: {e}")
                raise

    def local_simulation(self, start_date: str, end_date: str) -> None:
        self.clear_records(start_date, end_date)
        TradeCalendar.iterate_trade_days(start_date, end_date, self.strategy_simulation_daily_work)

    @abstractmethod
    def analyze_performance_datas(self, records: List[Tuple[TradeRecord,StrategyObservationEntry,ObservationVariable]]) -> Dict[str, Any]:
        """
        分析策略表现数据并返回格式化结果
        
        参数:
            records: 包含交易记录、观察条目和观察变量的元组列表
            
        返回:
            Dict[str, Any]: 包含分析结果的字典，例如:
                - 总交易数: 数据1
                - 胜率: 数据2
        """
        pass

    def analyze_strategy_performance(self, start_date: str, end_date: str):
        """
        收益分析
        """
        with StrategySession() as session:
            # 将输入日期格式化为 datetime 对象
            start_time = datetime.datetime.strptime(start_date, "%Y%m%d")
            end_time = datetime.datetime.strptime(end_date, "%Y%m%d") + datetime.timedelta(hours=23, minutes=59, seconds=59)

            total_days = (end_time - start_time).days+1  # 计算总天数
            if total_days <= 0:
                raise ValueError("起始日期必须早于结束日期")
            total_years = total_days / 365  # 转换为年

            # 从数据库中获取交易记录
            all_records = self.query_trade_record_data(session=session, filters=[
                TradeRecord.buy_time.between(start_time, end_time)
            ])
            if not all_records:
                logging.info(f"没有交易数据")
                return
            
            # 从all_records中提取buy_time并计算交易天数
            trade_days = len(set(
                record[0].buy_time.date()  # 获取TradeRecord的buy_time并转换为date
                for record in all_records
            ))

            valid_records = [record for record in all_records if record[0].sell_price!=None]
            if len(valid_records) < len(all_records) and self.run_mode == StrategyTaskMode.MODE_BACKTEST_LOCAL:
                logging.warning(f"{self.start_date} - {self.end_date} 有未结算的交易数据")              

            # 总交易数
            total_count = len(valid_records)

            # 分类盈利和亏损交易
            positive_trades = [record[0].profit_rate for record in valid_records if record[0].profit_rate > 0]
            negative_trades = [record[0].profit_rate for record in valid_records if record[0].profit_rate < 0]

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
            for record in valid_records:
                profit_rate = record[0].profit_rate / 100  # 将百分比转化为小数
                final_funds *= (1 + profit_rate)

            # 计算年化率
            cagr = ((float(final_funds) / float(initial_funds)) ** (1 / total_years) - 1) if total_years > 0 else None
            avg_buy_day = round(total_days / trade_days, 1) if trade_days > 0 else 0

            add_prints = self.analyze_performance_datas(valid_records)
            # 打印结果
            print(f"出手天数: {trade_days}")
            print(f"策略平均买入次数 {avg_buy_day} 天/次")
            print(f"股票池总数: {total_count}")
            print(f"策略涨跌幅>0的股票数: {positive_count}")
            print(f"胜率: {win_rate:.2%}")
            for key, value in add_prints.items():
                print(f"{key}: {value}")
            print(f"总盈利: {total_profit}")
            print(f"总亏损: {total_loss}")
            print(f"盈亏比: {profit_loss_ratio:.2f}" if profit_loss_ratio else "无亏损交易，无法计算盈亏比")
            print(f"初始资金: {initial_funds}")
            print(f"考虑复利后的累计资金量: {final_funds:.2f}")
            print(f"年化率: {cagr:.2%}" if cagr else "无法计算年化率")

    def export_trade_records_to_excel(self, start_date, end_date):
        """
        交易数据库导出至 Excel，包含 ObservationVariable 数据。
        """
        # 获取策略名称
        strategy_name = self.strategy_name()
        trade_record_path = get_project_path() / f"strategy_result/{strategy_name}_交易记录_{start_date}_{end_date}.xlsx"

        # 获取模型的字段描述作为列名
        observation_model = self.get_observation_model()
        observation_descriptions = observation_model.get_descriptions()
        excel_headers = ["股票代码", "股票名称", "购买日", "购买价格", "卖出日", "卖出价格", "盈利(%)"] + observation_descriptions

        # 初始化 Excel 文件管理器
        trade_file_manager = XLSXFileManager(trade_record_path, "历史交易记录", excel_headers, [0, 2], True)

        with StrategySession() as session:
            # 将输入日期格式化为 datetime 对象
            start_time = datetime.datetime.strptime(start_date, "%Y%m%d")
            end_time = datetime.datetime.strptime(end_date, "%Y%m%d") + datetime.timedelta(hours=23, minutes=59, seconds=59)

            # 查询 TradeRecord 数据
            results = self.query_trade_record_data(session=session, filters=[
                TradeRecord.buy_time.between(start_time, end_time),
                TradeRecord.sell_price != None
            ])

            for trade_record, observed_entry, observed_var in results:
                # 将 observed_var 转换为模型实例
                if observed_var:
                    observation_instance = observation_model.from_observation_variable(observed_var)
                    observation_values = list(observation_instance.model_dump().values())
                else:
                    observation_values = [None] * len(observation_descriptions)

                # 生成 Excel 数据记录
                excel_record = [
                    observed_entry.stock_code,
                    observed_entry.stock_name,
                    trade_record.buy_time.strftime("%Y-%m-%d %H:%M:%S"),
                    trade_record.buy_price,
                    trade_record.sell_time.strftime("%Y-%m-%d %H:%M:%S"),
                    trade_record.sell_price,
                    trade_record.profit_rate,
                ] + observation_values

                # 插入数据并保存
                trade_file_manager.insert_and_save(excel_record)

    def query_observation_data(
        self,
        session,
        include_observation_variable: bool = False,
        include_trade_record: bool = False,
        filters: List = None
    ) -> List[
        Union[
            Tuple[StrategyObservationEntry],
            Tuple[StrategyObservationEntry, ObservationVariable],
            Tuple[StrategyObservationEntry, ObservationVariable, TradeRecord],
            Tuple[StrategyObservationEntry, TradeRecord]
        ]
    ]:
        """
        查询策略数据，支持动态选择查询的对象。

        :param session: 数据库会话对象
        :param include_observation_variable: 是否查询 ObservationVariable
        :param include_trade_record: 是否查询 TradeRecord
        :param filters: 可选的额外查询条件（列表形式）
        :return: 查询结果列表，查询对象取决于所选参数
        """
        # 基础查询，始终包含 StrategyObservationEntry
        query_objects = [StrategyObservationEntry]
        join_conditions = []

        # 根据需要添加 ObservationVariable
        if include_observation_variable:
            query_objects.append(ObservationVariable)
            join_conditions.append(ObservationVariable)

        # 根据需要添加 TradeRecord
        if include_trade_record:
            query_objects.append(TradeRecord)
            join_conditions.append(TradeRecord)

        # 构造查询
        query = session.query(*query_objects)

        # 动态添加联结条件
        if include_observation_variable:
            query = query.join(
                ObservationVariable,
                StrategyObservationEntry.entry_id == ObservationVariable.entry_id,
            )
        if include_trade_record:
            query = query.outerjoin(
                TradeRecord,
                StrategyObservationEntry.entry_id == TradeRecord.entry_id
            )

        # 添加基本过滤条件
        query = query.filter(StrategyObservationEntry.version_id == self.version_id, StrategyObservationEntry.strategy_id == self.strategy_id)

        # 添加额外过滤条件
        if filters:
            query = query.filter(*filters)

        # 执行查询
        return query.all()
    
    def query_trade_record_data(
        self,
        session,
        filters: List = None
    ) -> List[
        Tuple[
            TradeRecord,
            StrategyObservationEntry,
            ObservationVariable
        ]
    ]:
        """
        查询交易记录数据，联结 StrategyObservationEntry 和 ObservationVariable，确保 ObservationVariable 存在。

        :param session: 数据库会话对象
        :param filters: 可选的额外查询条件（列表形式）
        :return: 查询结果列表，包含 TradeRecord、StrategyObservationEntry 和 ObservationVariable
        """
        # 构造查询
        query = session.query(
            TradeRecord,
            StrategyObservationEntry,
            ObservationVariable
        ).join(
            StrategyObservationEntry,
            TradeRecord.entry_id == StrategyObservationEntry.entry_id
        ).join(
            ObservationVariable,
            StrategyObservationEntry.entry_id == ObservationVariable.entry_id
        )

        # 添加基本过滤条件
        query = query.filter(
            TradeRecord.mode == self.run_mode,  # 示例：基于模式筛选
            StrategyObservationEntry.strategy_id == self.strategy_id,
            StrategyObservationEntry.version_id == self.version_id
        )

        # 添加额外过滤条件
        if filters:
            query = query.filter(*filters)

        # 执行查询
        return query.all()
    
    @abstractmethod
    def strategy_name(self) -> str:
        pass

    @abstractmethod
    def get_observation_model(self) -> Type[ObservationVarMode]:
        """
        返回与当前策略关联的 ObservationVariable 模型。
        子类需要实现。
        """
        pass