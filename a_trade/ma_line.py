# coding: utf-8

from typing import List, Optional
from a_trade.stocks_daily_data import StockDailyData
from a_trade.db_base import Session
import pandas as pd
from a_trade.trade_utils import MA
import logging
import a_trade.settings

def get_ma_line(ts_code: str = '', start_date: Optional[str] = None, 
                end_date: Optional[str] = None, ma: List[int] = []) -> Optional[pd.DataFrame]:
    """
    从数据库读取数据并计算均线
    Parameters:
    ------------
    ts_code: 证券代码，支持股票,ETF/LOF,期货/期权,港股,数字货币
    start_date: 开始日期  YYYYMMDD
    end_date: 结束日期 YYYYMMDD
    ma: 均线, 支持自定义均线频度，如：ma5/ma10/ma20/ma60/maN
    
    Return
    ----------
    DataFrame
    包含均线计算后的数据
    """
    session = Session()
    try:
        # 从数据库中获取日线数据
        query = session.query(StockDailyData).filter(
            StockDailyData.ts_code == ts_code,
            StockDailyData.trade_date.between(start_date, end_date)
        )
        df = pd.read_sql(query.statement, session.bind)

        # 计算均线
        if ma is not None and len(ma) > 0:
            for a in ma:
                if isinstance(a, int):
                    df['ma%s' % a] = MA(df['close'], a)
                    # df['close'].rolling(window=a).mean().shift(-(a - 1))
        
        return df
    except Exception as e:
        print(e)
        return None
