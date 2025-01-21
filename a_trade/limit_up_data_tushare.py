# coding: utf-8
# 文件：limit_up_data_tushare.py

import os
import datetime
import logging
import time
import requests
import pywencai
import pandas as pd
from typing import Optional
import sys

from a_trade.settings import _get_tushare
from a_trade.trade_calendar import TradeCalendar
from a_trade.stocks_daily_data import get_previous_trade_date, StockDailyData
from sqlalchemy import Column, String, Float, Integer, func
from a_trade.db_base import Session, get_recent_trade_date_in_table,Base
from a_trade.trade_utils import code_with_exchange, timestamp_in_millis, strip_stock_name

# 获取 pywencai 的日志记录器
pywencai_logger = logging.getLogger('pywencai')  # 根据实际模块名调整

# 创建文件处理器
file_handler = logging.FileHandler('pywencai.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('[pywencai] %(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 添加处理器到 pywencai 的日志记录器
pywencai_logger.addHandler(file_handler)

# 确保 pywencai 的日志级别合适
pywencai_logger.setLevel(logging.INFO)

# 定义数据库模型
class LimitUpTushare(Base):
    __tablename__ = 'limit_up_tushare'
    trade_date = Column(String, primary_key=True)
    stock_code = Column(String, primary_key=True)
    stock_name = Column(String)
    industry = Column(String)
    close = Column(Float)
    pct_chg = Column(Float)
    amount = Column(Float)
    limit_amount = Column(Float)
    float_mv = Column(Float)
    total_mv = Column(Float)
    turnover_ratio = Column(Float)
    fd_amount = Column(Float)
    first_time = Column(String)
    last_time = Column(String)
    open_times = Column(Integer)
    up_stat = Column(String)
    limit_times = Column(Integer)
    limit_status = Column(String)
    start_date = Column(String)
    end_date = Column(String)
    continuous_limit_up_count = Column(Integer)
    reason_type = Column(String)

    def get_daily_data(self):    
        with Session() as session:
            daily_data = session.query(StockDailyData).filter(
                StockDailyData.ts_code == self.stock_code,
                StockDailyData.trade_date == self.trade_date
            ).first()
            return daily_data

    def limit_status_desc(self):
        if self.limit_status == 'U':
            return '涨停'
        if self.limit_status == 'D':
            return '跌停'
        if self.limit_status == 'Z':
            daily_data = self.get_daily_data()
            if daily_data.close == daily_data.high:
                return '涨停'
            return '炸板'

    def __repr__(self):
        return f"<LimitUpTushare(stock_code={self.stock_code}, trade_time={self.trade_date}, stock_name={self.stock_name})>"

class LimitDataSource():
    def __init__(self, trade_date):
        self.trade_date = trade_date
        with Session() as session:
            result = session.query(LimitUpTushare).filter(
                LimitUpTushare.trade_date == trade_date
            ).order_by(LimitUpTushare.first_time).all()
            # 涨停数据字典
            self.limit_up_map = {
                record.stock_code: record
                for record in result
                if record.limit_status == 'U'  # 'U' 表示涨停
            }

            # 炸板数据字典
            self.limit_failed_map = {
                record.stock_code: record
                for record in result
                if record.limit_status == 'Z'  # 'Z' 表示炸板
            }

            # 跌停数据字典
            self.limit_down_map = {
                record.stock_code: record
                for record in result
                if record.limit_status == 'D'  # 'D' 表示跌停
            }
            
            self.all_limit_map = {
                record.stock_code: record
                for record in result
            }

            # 初始化内存缓存
            self.pct_chg_cache = {}
            self.daily_data_cache = {}
            self.load_daily_data()

    def get_name_to_code(self):
        """
        返回股票名称到股票代码的映射字典
        """
        return {
            record.stock_name: record.stock_code
            for record in self.all_limit_map.values()
        }

    def load_daily_data(self):
        with Session() as session:
            stock_codes = self.all_limit_map.keys()
            datas = session.query(StockDailyData).filter(
                StockDailyData.trade_date == self.trade_date,
                StockDailyData.ts_code.in_(stock_codes)
            ).all()
            self.daily_data_cache = {
                data.ts_code: data
                for data in datas
            }

    def get_daily_data(self, stock_code):
        if stock_code in self.daily_data_cache:
            return self.daily_data_cache[stock_code]
        
        with Session() as session:
            daily_data = session.query(StockDailyData).filter(
                StockDailyData.ts_code == stock_code,
                StockDailyData.trade_date == self.trade_date
            ).first()
            if daily_data:
                self.daily_data_cache[stock_code] = daily_data
                return daily_data
        assert False, f"{stock_code} {self.trade_date}没有日线数据"

    def get_pct_chg(self, stock_code):
        """
        获取指定股票在指定周期内的涨幅（带缓存）。
        
        :param stock_code: 股票代码
        :param days: 涨幅计算的天数
        :return: 涨幅值（百分比）
        """
        limit_data = self.all_limit_map.get(stock_code)
        days_ago = 0
        if limit_data.up_stat:
            boards, days_ago = map(int, limit_data.up_stat.split('/'))

        if not limit_data.up_stat or days_ago == 0:
            logging.info(f"{limit_data.stock_name} {days_ago}天{boards}板 累计涨幅 0.0")
            return 0.0

        # 检查缓存
        if (stock_code, days_ago) in self.pct_chg_cache:
            logging.info(f"{limit_data.stock_name} {days_ago}天{boards}板 累计涨幅 {self.pct_chg_cache[stock_code]}")
            return self.pct_chg_cache[stock_code]

        # 查询数据库，获取 start_date 和 trade_date 的收盘价
        with Session() as session:
            daily_data = session.query(StockDailyData).filter(
                StockDailyData.ts_code == stock_code,
                StockDailyData.trade_date <= self.trade_date
            ).order_by(StockDailyData.trade_date.desc()).limit(days_ago + 1).all()
        # 检查数据完整性
        if len(daily_data) < days_ago:
            logging.error(f"股票 {stock_code} 在 {self.trade_date} 之前的数据不足 {days_ago} 天，无法计算涨幅")
            return 0.0
        
        # 检查是否包含跌停数据
        for day in daily_data:
            if day.pct_chg and day.pct_chg <= -9.5:
                logging.info(f"股票 {stock_code} 包含跌停数据，累计涨幅视为 0.0")
                self.pct_chg_cache[stock_code] = 0.0
                return 0.0

        if len(daily_data) == days_ago:
            # 新股情况：取第一天开盘价作为起始值
            logging.info(f"股票 {stock_code} 是新股，累计涨幅视为 0.0")
            self.pct_chg_cache[stock_code] = 0.0
            return 0.0
            # start_data = daily_data[-1]
            # close_start = start_data.open
        else:
            # 正常情况：取倒数第 days_ago+1 天的收盘价作为起始值
            start_data = daily_data[-1]
            close_start = start_data.close if start_data.close else start_data.open

        close_end = daily_data[0].close     # 最近一个交易日的收盘价

        # 计算涨幅
        pct_chg = round((close_end - close_start) / close_start * 100, 2)

        # 缓存结果
        self.pct_chg_cache[stock_code] = pct_chg
        logging.info(f"{limit_data.stock_name} {days_ago}天{boards}板 累计涨幅 {self.pct_chg_cache[stock_code]}")
        return pct_chg

    def compare_stock_strength(self, stock_code_a, stock_code_b):
        """
        比较两只股票的强度。

        :param stock_code_a: 股票A代码
        :param stock_code_b: 股票B代码
        :return: 强度更强的股票代码
        """
        if not stock_code_a:
            return stock_code_b
        if not stock_code_b:
            return stock_code_a
        
        stock_a = self.all_limit_map.get(stock_code_a)
        stock_b = self.all_limit_map.get(stock_code_b)
        # print(self.all_limit_map)
        if not stock_a or not stock_b:
            raise ValueError(f"股票代码{stock_code_a} {stock_code_b} {self.trade_date}不存在或不在交易数据中")

        # 在涨停字典里的股票比不在涨停里的强
        is_a_limit_up = stock_code_a in self.limit_up_map
        is_b_limit_up = stock_code_b in self.limit_up_map
        if not is_a_limit_up and not is_b_limit_up:
            return stock_code_a
        if is_a_limit_up != is_b_limit_up:
            return stock_code_a if is_a_limit_up else stock_code_b
        
        # 比较 up_stat
        logging.info(f"{stock_a.stock_name} 封板时间 {stock_a.last_time} 连板数 {stock_a.continuous_limit_up_count}")
        logging.info(f"{stock_b.stock_name} 封板时间 {stock_b.last_time} 连板数 {stock_b.continuous_limit_up_count}")
        if stock_a.continuous_limit_up_count == stock_b.continuous_limit_up_count:
            stock_daily_a: Optional[StockDailyData] = self.get_daily_data(stock_code_a)
            stock_daily_b: Optional[StockDailyData] = self.get_daily_data(stock_code_b)
            is_t_limit_a = stock_daily_a.is_t_limit()
            is_t_limit_b = stock_daily_b.is_t_limit()
            if is_t_limit_a == is_t_limit_b:
                return stock_code_a if int(stock_a.last_time) < int(stock_b.last_time) else stock_code_b
            else:
                return stock_code_a if is_t_limit_a else stock_code_b
            
        return stock_code_a if stock_a.continuous_limit_up_count > stock_b.continuous_limit_up_count else stock_code_b
    
    def compare_stock_recent_height(self, stock_code_a, stock_code_b):
        if not stock_code_a:
            return stock_code_b
        if not stock_code_b:
            return stock_code_a
        
        stock_a = self.all_limit_map.get(stock_code_a)
        stock_b = self.all_limit_map.get(stock_code_b)
        if not stock_a or not stock_b:
            raise ValueError(f"股票代码{stock_code_a} {stock_code_b} {self.trade_date}不存在或不在交易数据中")

        # 10cm 的股票比 20cm 的强
        # cm_a = is_10cm_stock(stock_code_a)
        # cm_b = is_10cm_stock(stock_code_b)
        # if not cm_a and not cm_b:
        #     return stock_code_a
        # if cm_a != cm_b:
        #     return stock_code_a if cm_a else stock_code_b
        
         # 获取涨幅
        pct_chg_a = self.get_pct_chg(stock_code_a)
        pct_chg_b = self.get_pct_chg(stock_code_b)
        return stock_code_a if pct_chg_a > pct_chg_b else stock_code_b
    
def is_strong_stock_base_limit_data(data: LimitUpTushare):
    if data:
        return data.limit_status == 'U' and data.open_times < 1
    return False

def find_recent_limit_up(stock_code: str, trade_date: str, pre_range: int) -> Optional[LimitUpTushare]:
    """
    查找指定股票在指定交易日之前 pre_range 个交易日内的最近一个涨停数据。

    :param stock_code: 股票代码
    :param trade_date: 交易日 (格式: YYYYMMDD)
    :param pre_range: 向前查找的交易日范围
    :return: 最近一个涨停数据 (LimitUpTushare 实例) 或 None
    """
    with Session() as session:
        # 获取交易日列表
        previous_trade_day = TradeCalendar.get_previous_trade_date(trade_date, pre_range)

        if not previous_trade_day:
            logging.info(f"No trade days found for range {pre_range} before {trade_date}.")
            return None

        # 查询最近的涨停数据
        result = (
            session.query(LimitUpTushare)
            .filter(LimitUpTushare.stock_code == stock_code)
            .filter(LimitUpTushare.trade_date >= previous_trade_day)
            .filter(LimitUpTushare.trade_date < trade_date)
            .filter(LimitUpTushare.limit_status == 'U')  # 过滤涨停状态
            .order_by(LimitUpTushare.trade_date.desc())  # 按日期倒序排列，找到最近的
            .first()
        )

        return result
    
def get_limit_info(stock_code: str, trade_date: str) -> Optional[LimitUpTushare]:
    with Session() as session:
        
        # 查询最近的涨停数据
        result = (
            session.query(LimitUpTushare)
            .filter(LimitUpTushare.stock_code == stock_code)
            .filter(LimitUpTushare.trade_date == trade_date)
            .first()
        )

        return result

def can_shrink_date_range(start_date, end_date):
    start = datetime.datetime.strptime(start_date, "%Y%m%d")
    end = datetime.datetime.strptime(end_date, "%Y%m%d")
    return (end - start).days > 1

def update_limit_up_data_until(end_date):
    start_date = get_recent_trade_date_in_table(LimitUpTushare.__tablename__)
    update_limit_up_data(start_date, end_date)
    return start_date

def update_limit_up_data(start_date, end_date):
    if not TradeCalendar.validate_date_range(start_date, end_date):
        return

    try:
        logging.debug(f"正在从tushare获取日期范围 {start_date} 到 {end_date} 的涨停数据")
        limit_up_data = _get_tushare().pro_api().limit_list_d(
            start_date=start_date,
            end_date=end_date,
            fields='trade_date,ts_code,industry,name,close,pct_chg,amount,limit_amount,float_mv,total_mv,turnover_ratio,fd_amount,first_time,last_time,open_times,up_stat,limit_times,limit'
        )
        data_length = len(limit_up_data)
        logging.info(f"完成获取日期范围 {start_date} 到 {end_date} 的涨停数据，数据长度: {data_length}")
        if data_length == 0:
            sys.exit(1)

        # if data_length > 1000 and can_shrink_date_range(start_date, end_date):
        #     raise ValueError("返回的数据超过1000条，请缩小日期范围。")

        if limit_up_data is not None:
            _write_to_db(limit_up_data, start_date, end_date)
    except Exception as e:
        sys.exit(1)
        logging.error(f"limit_list_d接口 数据获取失败: {e}")
    
    update_continuous_limit_up_count(start_date, end_date)

def _write_to_db(data, start_date, end_date):
    session = Session()
    try:
        for _, row in data.iterrows():
            new_record = LimitUpTushare(
                trade_date=row['trade_date'],
                stock_code=row['ts_code'],
                stock_name=strip_stock_name(row['name']),
                industry=row['industry'],
                close=row['close'],
                pct_chg=row['pct_chg'],
                amount=row['amount'],
                limit_amount=row['limit_amount'],
                float_mv=row['float_mv'],
                total_mv=row['total_mv'],
                turnover_ratio=row['turnover_ratio'],
                fd_amount=row['fd_amount'],
                first_time=row['first_time'],
                last_time=row['last_time'],
                open_times=row['open_times'],
                up_stat=row['up_stat'],
                limit_times=row['limit_times'],
                limit_status=row['limit'],
                start_date=start_date,
                end_date=end_date,
                continuous_limit_up_count=0  # 初始化为0
            )
            session.merge(new_record)  # 使用merge方法保证主键重复时更新现有记录
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"数据写入失败: {e}")
    finally:
        session.close()

def update_continuous_limit_up_count(start_date, end_date):
    if not TradeCalendar.validate_date_range(start_date, end_date):
        return
    
    def update_continuous_limit_up_count_for_day(trade_date):
        logging.info(f"计算 {trade_date} 连板数")
        previous_date = TradeCalendar.get_previous_trade_date(trade_date)
        today_data_source = LimitDataSource(trade_date)
        pre_data_source = LimitDataSource(previous_date)

        continuous_count = 0
        update_batch = []
        for stock_code, stock_record in today_data_source.limit_up_map.items():
            if stock_code in pre_data_source.all_limit_map:      
                previous_record = pre_data_source.all_limit_map[stock_code]
                continuous_count = previous_record.continuous_limit_up_count + 1
            else:
                # 如果默认上一交易日无数据，使用动态查询上一有效交易日
                actual_previous_date = get_previous_trade_date(trade_date, stock_code)
                if actual_previous_date and actual_previous_date != previous_date:
                    logging.info(f"{trade_date} {stock_code} 存在停牌，上一有效交易日 {actual_previous_date}")
                    actual_previous_data_source = LimitDataSource(actual_previous_date)
                    if stock_code in actual_previous_data_source.all_limit_map:
                        previous_record = actual_previous_data_source.all_limit_map[stock_code]
                        continuous_count = previous_record.continuous_limit_up_count + 1
                    else:
                        continuous_count = 1
                else:
                    continuous_count = 1

            # 更新当前股票的连续涨停计数
            update_batch.append({
                'trade_date': stock_record.trade_date,
                'stock_code': stock_code,
                'continuous_limit_up_count': continuous_count
            })

        if update_batch:
            session = Session()
            try:
                session.bulk_update_mappings(LimitUpTushare, update_batch)
                session.commit()
            except Exception as e:
                session.rollback()
                logging.error(f"更新连板计数失败: {e}")
            finally:
                session.close()

    TradeCalendar.iterate_trade_days(start_date, end_date, update_continuous_limit_up_count_for_day)
    
def fetch_reason_types_from_limitpool(start_date, end_date):
    TradeCalendar.iterate_trade_days(start_date, end_date, fetch_reason_types_from_limitpool_for_day)

def fetch_reason_types_from_limitpool_for_day(current_date):
    _JKQA_HEADER = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'data.10jqka.com.cn',
        'Referer': 'https://data.10jqka.com.cn/datacenterph/limitup/limtupInfo.html?fontzoom=no&client_userid=cA2fp&share_hxapp=gsc&share_action=webpage_share.1&back_source=wxhy',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36'
    }
    url = "https://data.10jqka.com.cn/dataapi/limit_up/limit_up_pool"
    page = 1
    while True:
        logging.debug(f"正在请求{current_date}涨停原因数据第{page}页数据")
        params = {
            "page": str(page),
            "limit": "100",
            "field": "199112,10,9001,330323,330324,330325,9002,330329,133971,133970,1968584,3475914,9003,9004",
            "filter": "HS,GEM2STAR",
            "order_field": "330325",
            "order_type": "0",
            "date": current_date,
            "_": timestamp_in_millis('2024-01-01')  # 添加时间戳参数，避免缓存
        }
        try:
            time.sleep(0.5) # 间隔500ms爬取数据，防止频繁爬取被封禁
            response = requests.get(url, headers=_JKQA_HEADER, params=params)
            response.raise_for_status()
            data = response.json()
            logging.debug(data)

            if "data" in data and "info" in data["data"]:
                session = Session()
                limit_data_source = LimitDataSource(trade_date)
                for item in data["data"]["info"]:
                    reason_type = item.get("reason_type", "N/A")
                    code = item.get("code")
                    trade_date = current_date
                    stock_code = code_with_exchange(code)

                    # 更新数据库中的 reason_type 字段
                    existing_record = limit_data_source.all_limit_map[stock_code]
                    
                    if existing_record:
                        existing_record.reason_type = reason_type
                        logging.info(f"数据表limit_up_tushare找到:{trade_date}股票:{stock_code}的涨停数据，涨停原因{reason_type}")
                        session.merge(existing_record)
                    else:
                        logging.error(f"数据表limit_up_tushare未找到:{trade_date}股票:{stock_code}的涨停数据，涨停原因{reason_type}")
                session.commit()
                session.close()
            else:
                logging.error(f"Date: {current_date}, No data available")

            # 检查分页信息，决定是否继续请求下一页
            if "data" in data and "page" in data["data"]:
                page_info = data["data"]["page"]
                if page * page_info["limit"] >= page_info["total"]:
                    break
                else:
                    page += 1
            else:
                break

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for date {current_date}, page {page}: {e}")
            break

def fetch_reason_types_from_wencai_for_day(current_date):
    logging.info(f"正在请求{current_date}涨停原因数据")
    session = Session()
    
   # 第一部分：尝试拉取数据
    try:
        res = pywencai.get(
            query=f'{current_date},涨停,市值降序', 
            perpage=100, 
            loop=True, 
            query_type='stock', 
            sleep=0.5,
            log=True
        )
        logging.info(f"拉取到的数据: {res}")
        
        # 验证返回值是否有效
        if res is None or not isinstance(res, pd.DataFrame) or res.empty:
            raise ValueError("获取到的数据为空或格式不正确，请检查查询条件或网络状况")
        
    except ValueError as ve:
        # 处理返回数据为空的情况
        logging.error(f"数据拉取为空，日期 {current_date}: {ve}")
        session.close()
        return
    
    except Exception as e:
        # 如果拉取数据时发生其他异常，记录日志并退出函数
        logging.error(f"拉取数据时发生错误，日期 {current_date}: {e}")
        session.close()
        return

    # 检查拉取的数据是否为空
    if res.empty:
        logging.error(f"日期 {current_date}: 无法获取数据")
        session.close()
        return

    # 数据处理和写入数据库
    limit_data_source = LimitDataSource(current_date)

    try:
        for _, row in res.iterrows():
            reason_type = row.get(f'涨停原因类别[{current_date}]', "N/A")
            stock_code = row.get('股票代码')
            stock_name = row.get('股票简称')

            if not stock_code:
                logging.warning(f"数据缺失: 股票代码为空，跳过此条记录 {row}")
                continue

            # 更新数据库中的 reason_type 字段
            existing_record = limit_data_source.all_limit_map.get(stock_code)
            if existing_record:
                existing_record.reason_type = reason_type
                session.merge(existing_record)
            else:
                logging.warning(
                    f"数据表 limit_up_tushare 未找到记录: 日期 {current_date}, 股票代码 {stock_code}, "
                    f"股票名称 {stock_name}, 涨停原因 {reason_type}"
                )
        session.commit()
        logging.info(f"成功更新数据库，日期 {current_date}")

    except Exception as e:
        # 如果写入数据库时发生异常，记录日志并回滚事务
        logging.error(f"写入数据库时发生错误，日期 {current_date}: {e}")
        session.rollback()
    finally:
        session.close()

def process_frequent_reasons_during(start_date, end_date):
    TradeCalendar.iterate_trade_days(start_date, end_date, process_frequent_reasons_for_day)

def process_frequent_reasons_for_day(current_date):
    """
    根据日期处理高频涨停原因，并将新增的原因写入自定义板块中。
    """
    limit_data_source = LimitDataSource(current_date)
    session = Session()
    # 从数据库中查询指定日期的涨停原因
    if not limit_data_source.limit_up_map:
        logging.info(f"日期 {current_date} 无涨停原因数据")
        return

    # 统计涨停原因出现次数
    reason_counts = {}
    for _, record in limit_data_source.limit_up_map.items():
        reason_type = record.reason_type
        if reason_type:
            first_reason = reason_type.split('+')[0].strip()  # 提取第一个原因
            if first_reason:
                reason_counts[first_reason] = reason_counts.get(first_reason, 0) + 1

    # 筛选出高频原因（出现次数 >= 3）
    frequent_reasons = [reason for reason, count in reason_counts.items() if count >= 3]

    if frequent_reasons:
        try:
            from a_trade.reason_concept import reasonConceptManager, LimitReasonToConcept
            custom_concepts = reasonConceptManager.custom_concept_words

            # 查询数据库中已有的原因
            existing_reasons = session.query(LimitReasonToConcept.limit_reason).filter(
                LimitReasonToConcept.limit_reason.in_(frequent_reasons)
            ).all()

            # 提取已有原因的集合
            existing_reasons_set = {reason[0] for reason in existing_reasons}

            # 筛选出新增的高频原因
            new_reasons = [
                reason for reason in frequent_reasons 
                if reason not in existing_reasons_set and reason not in custom_concepts
            ]
            if new_reasons:
                logging.info(f"{current_date} 有新增高频原因，暂时按照自定义板块处理 {new_reasons}")
                for new_reason in new_reasons:
                    # 保存所有新增板块到 JSON 文件
                    reasonConceptManager.add_concept_keywords(new_reason, new_reason)
            else:
                logging.info(f"{current_date} 没有新增自定义板块")
        except Exception as e:
            logging.error(f"处理高频原因时发生错误，日期 {current_date}: {e}")
        finally:
            # 确保会话关闭
            session.close()
    else:
        logging.info(f"{current_date} 没有新增自定义板块")

def fetch_reason_types_from_wencai(start_date, end_date):
    TradeCalendar.iterate_trade_days(start_date, end_date, fetch_reason_types_from_wencai_for_day)

def fetch_reason_types_from_wencai_util(end_date):    
    last_full_empty_date = None

    with Session() as session:
        # 从 end_date 开始向前无限遍历
        current_date = end_date
        while True:            
            # 查询当天是否所有 'reason_type' 均为空
            total_records = session.query(LimitUpTushare).filter(LimitUpTushare.trade_date == current_date).count()
            empty_records = session.query(LimitUpTushare).filter(LimitUpTushare.trade_date == current_date, LimitUpTushare.reason_type == None).count()
            logging.info(f"{current_date} 涨跌停总数:{total_records}, 涨停原因为空总数:{empty_records}")
            # 如果当天所有记录的 'reason_type' 都为空
            if total_records > 0:
                # 有交易数据
                if total_records == empty_records:
                    last_full_empty_date = current_date
                elif not last_full_empty_date:
                    # end_date已经有涨停原因数据
                    return None
                else:
                    # 涨停原因数据不全空，中止循环
                    break
            current_date = TradeCalendar.get_previous_trade_date(current_date)

        if last_full_empty_date:
            logging.info(f"{last_full_empty_date} 尚未有涨停原因数据，即将更新wencai数据")
            fetch_reason_types_from_wencai(last_full_empty_date, end_date)
        else:
            print("No date found where all 'reason_type' are empty.")
    return last_full_empty_date

if __name__ == "__main__":
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    
    # process_frequent_reasons_during(start_date, end_date)

    # update_limit_up_data(start_date, end_date)
    # update_continuous_limit_up_count(start_date, end_date)
    fetch_reason_types_from_wencai(start_date, end_date)
    # update_limit_up_data_until(end_date)
    # limit_data_source = LimitDataSource('20250115')
    # daily_data = limit_data_source.get_daily_data('605033.SH')
    # print(daily_data.is_t_limit())