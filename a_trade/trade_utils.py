# coding: utf-8
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Union, List, Dict, Any

def is_10cm_stock(stock_code: str) -> bool:
    # 判断是否为深圳创业板股票，涨跌幅20%
    if stock_code.startswith("3"):
        return False
    # 判断是否为科创板股票或科创板存托凭证，涨跌幅20%
    elif stock_code.startswith("688") or stock_code.startswith("689"):
        return False
    # 判断是否为上海或深圳的主板或中小板，涨跌幅10%
    elif stock_code.startswith("0") or stock_code.startswith("6"):
        return True
    # 其他情况，默认不是涨跌幅10%的股票
    return False

def code_with_exchange(stock_code: str) -> str:
    if stock_code.startswith("4"):
        return f"{stock_code}.BJ"
    elif stock_code.startswith("0") or stock_code.startswith("3"):
        return f"{stock_code}.SZ"
    elif stock_code.startswith("6"):
        return f"{stock_code}.SH"
    else:
        logging.error(f"未知交易所,代码{stock_code}")
        return "未知交易所"
    
def timestamp_in_millis(date: Optional[Union[str, pd.Timestamp]] = None) -> int:
    """
    获取指定日期的时间戳（毫秒级）。如果不传入日期，则返回当前时间的时间戳。
    
    参数:
    - date (str 或 pd.Timestamp): 指定日期，格式可以为 "YYYY-MM-DD" 或 Pandas Timestamp 对象。
    
    返回:
    - int: 指定日期的毫秒级 Unix 时间戳。
    """
    if date is None:
        # 没有传入日期时，使用当前 UTC 时间
        timestamp = pd.Timestamp.utcnow()
    else:
        # 传入日期时，解析日期
        timestamp = pd.Timestamp(date)
        
    return int(timestamp.timestamp() * 1000)

def format_time_string(time_str: str) -> str:
    # 将数字字符串转换为时间格式
    if len(time_str) == 5:
        time_str = '0' + time_str
    formatted_time = f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:]}"
    return formatted_time

def format_amount_string(amount: float) -> str:
    # 将金额格式化为"xx亿""
    return f"{amount / 1e8:.2f}亿"

def format_limit_up_count_string(limit_up_count_str: str) -> str:
    # 将金额格式化为"xx亿""
    array = limit_up_count_str.split('/')
    if len(array) == 2:
        return f"{array[1]}天{array[0]}板"
    else:
        return "格式化异常"

def MA(DF: pd.Series, N: int) -> pd.Series:
    return pd.Series.rolling(DF, N).mean()

def concepts_equal(array1: List[str], array2: List[str]) -> bool:
    if not array1 and not array2:
        return False
    if len(array1) != len(array2):
        return False
    return sorted(array1) == sorted(array2)

def strip_list(origin_list: List[Any]) -> List[Any]:
    return list(dict.fromkeys(origin_list))

def strip_code_suffix(stock_code: str) -> str:
    if "." in stock_code:
        return stock_code.split(".")[0]
    return stock_code

def strip_stock_name(stock_name: Optional[str]) -> Optional[str]:
    if not stock_name:
        return stock_name
    return stock_name.replace(" ", "")

def time_difference_less_than(first_time: Optional[str], last_time: Optional[str], seconds: int = 60) -> bool:
    if first_time is None or last_time is None:
        return False
    # 设定时间格式
    time_format = '%H%M%S'
    
    # 将时间字符串转换为datetime对象
    first_time_obj = datetime.strptime(first_time, time_format)
    last_time_obj = datetime.strptime(last_time, time_format)
    
    # 计算两个时间之间的差异
    time_difference = abs(last_time_obj - first_time_obj)
    
    # 检查时间差是否小于30秒
    return time_difference < timedelta(seconds=seconds)

if __name__ == "__main__":
    print(strip_stock_name('特  力Ａ'))