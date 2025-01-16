# coding: utf-8
from sqlalchemy import Column, String, Float, Integer, Date
import tushare as ts
import datetime
from a_trade.db_base import Session, Base, engine
from a_trade.settings import _get_tushare
import logging
import sys

# 定义分时数据表
class StockMinuteData(Base):
    __tablename__ = 'stock_minute_data'
    stock_code = Column(String, primary_key=True)
    trade_time = Column(String, primary_key=True)
    close = Column(Float)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    vol = Column(Float)
    amount = Column(Float)
    trade_date = Column(String)
    pre_close = Column(Float)
    change = Column(Float)
    pct_chg = Column(Float)
    avg = Column(Float)

    def __repr__(self):
        return f"<MinuteData(stock_code={self.stock_code}, trade_time={self.trade_time}, close={self.close}, avg={self.avg})>"
    
    def __getitem__(self, key):
        if key == 'close':
            return self.close
        elif key == 'open':
            return self.open
        elif key == 'avg':
            return self.avg
        elif key == 'trade_time':
            return self.trade_time
        elif key == 'high':
            return self.high
        elif key == 'low':
            return self.low
        elif key == 'bob':
            return datetime.datetime.strptime(self.trade_time, "%Y-%m-%d %H:%M:%S")
        elif key == 'amount':
            return self.amount
        elif key == 'volume':
            return self.vol
        else:
            raise KeyError(f"Invalid key: {key}")
        
# 创建表格（如果不存在的话）
Base.metadata.create_all(engine)

# 方法1: 获取并保存数据
def fetch_and_save_data(stock_code, trade_date):
    # 将传入的日期字符串转换为datetime对象
    date_object = datetime.datetime.strptime(trade_date, "%Y%m%d")
    
    # 格式化为需要的开始时间和结束时间，包含具体时间
    start_datetime = datetime.datetime(date_object.year, date_object.month, date_object.day, 9, 0)  # 当天的09:00
    end_datetime = datetime.datetime(date_object.year, date_object.month, date_object.day, 15, 0)  # 当天的15:00
    
    # 转换为字符串格式"YYYY-MM-DD HH:MM:SS"，适用于分钟级数据请求
    start_date_formatted = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    end_date_formatted = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"调用Tushare 获取{trade_date} {stock_code} {start_date_formatted} - {end_date_formatted} 分时数据")
    session = Session()
    try:
        df = ts.pro_bar(ts_code=stock_code, freq='1min', start_date=start_date_formatted, end_date=end_date_formatted)
        
        # 按 trade_time 升序排序
        df = df.sort_values(by='trade_time').reset_index(drop=True)
        print(df)
        if len(df) == 0:
            sys.exit(1)

        # 初始化累计数据
        total_amount = 0
        total_vol = 0
        
        for index, row in df.iterrows():
            # 累计计算总金额和总成交量
            total_amount += row['amount']
            total_vol += row['vol']

            # 计算动态平均成交价
            avg_price = round(total_amount / total_vol, 2) if total_vol > 0 else 0

            minute_data = StockMinuteData(
                stock_code=row['ts_code'],
                trade_time=row['trade_time'],
                close=row['close'],
                open=row['open'],
                high=row['high'],
                low=row['low'],
                vol=row['vol'],
                amount=row['amount'],
                trade_date=row['trade_date'],
                pre_close=row['pre_close'],
                change=row['change'],
                pct_chg=row['pct_chg'],
                avg=avg_price  # 设置平均成交价
            )
            session.add(minute_data)
        session.commit()
    except Exception as e:
        sys.exit(1)
        logging.error(e)
    finally:
        session.close()

# 方法2: 获取数据
def get_minute_data(stock_code, trade_date):
    with Session() as session:
        result = session.query(StockMinuteData).filter_by(stock_code=stock_code, trade_date=trade_date).order_by(StockMinuteData.trade_time).all()
        if not result:
            fetch_and_save_data(stock_code, trade_date)
            result = session.query(StockMinuteData).filter_by(stock_code=stock_code, trade_date=trade_date).order_by(StockMinuteData.trade_time).all()
        return result
    
def get_minute_data_for_multiple_stocks(stock_codes, trade_date):
    """
    获取多个股票在指定交易日的分钟数据。如果数据不存在，会调用 `fetch_and_save_data` 进行拉取。

    参数:
        stock_codes: list，股票代码列表
        trade_date: string，交易日期，例如 "20240102"

    返回:
        dict，每个股票代码对应的分钟数据列表，例如:
        {
            "000001": [StockMinuteData对象, ...],
            "000002": [StockMinuteData对象, ...],
        }
    """
    with Session() as session:
        # 查询所有股票的数据
        result = session.query(StockMinuteData).filter(
            StockMinuteData.stock_code.in_(stock_codes),
            StockMinuteData.trade_date == trade_date
        ).order_by(StockMinuteData.stock_code, StockMinuteData.trade_time).all()

        # 将查询结果按股票代码分组
        data_by_stock = {stock_code: [] for stock_code in stock_codes}
        for record in result:
            data_by_stock[record.stock_code].append(record)

        # 检查是否有缺失数据的股票
        missing_stocks = [stock_code for stock_code in stock_codes if not data_by_stock[stock_code]]

        # 拉取缺失股票的数据
        for stock_code in missing_stocks:
            fetch_and_save_data(stock_code, trade_date)
            # 再次查询缺失的数据并填充
            fetched_data = session.query(StockMinuteData).filter_by(stock_code=stock_code, trade_date=trade_date).order_by(StockMinuteData.trade_time).all()
            data_by_stock[stock_code] = fetched_data

        return data_by_stock

# 方法3: 判断强势涨停板
def is_strong_limit_up_base_minute_data(stock_code, trade_date, limit_price, start_time, end_time, weak_limit_open_time=10):
    data = get_minute_data(stock_code, trade_date)

    # 确保时间字符串是六位长
    start_time = f"{int(start_time):06d}"  # 前导零填充至六位
    end_time = f"{int(end_time):06d}"

    # 格式化 start_time 和 end_time
    start_time_formatted = datetime.datetime.strptime(f"{trade_date} {start_time[:2]}:{start_time[2:4]}:{start_time[4:]}", "%Y%m%d %H:%M:%S")
    end_time_formatted = datetime.datetime.strptime(f"{trade_date} {end_time[:2]}:{end_time[2:4]}:{end_time[4:]}", "%Y%m%d %H:%M:%S")
    logging.info(f"{trade_date} - {stock_code} - {start_time_formatted} - {end_time_formatted}")
    
    filtered_data = [d for d in data if start_time_formatted <= datetime.datetime.strptime(d.trade_time, "%Y-%m-%d %H:%M:%S") <= end_time_formatted]
    non_limit_minutes = [d for d in filtered_data if d.high < limit_price]
    is_strong = len(non_limit_minutes) < weak_limit_open_time
    logging.info(f"{trade_date} {stock_code} 分时强势板判断, {start_time_formatted} - {end_time_formatted}之间,开板时长{len(non_limit_minutes)}分钟")
    return is_strong

def calculate_avg_price(trade_date):
    with Session() as session:
        # 获取指定交易日的数据，并按 `stock_code` 和 `trade_time` 排序
        minute_data = session.query(StockMinuteData).filter(
            StockMinuteData.trade_date == trade_date
        ).order_by(StockMinuteData.stock_code, StockMinuteData.trade_time).all()

        if not minute_data:
            print(f"交易日 {trade_date} 没有数据")
            return

        # 按股票代码分组
        stock_data_by_code = {}
        for data in minute_data:
            if data.stock_code not in stock_data_by_code:
                stock_data_by_code[data.stock_code] = []
            stock_data_by_code[data.stock_code].append(data)

        # 更新每个股票的 avg
        for _, data_list in stock_data_by_code.items():
            total_amount = 0
            total_vol = 0

            for data in data_list:
                total_amount += data.amount
                total_vol += data.vol
                avg_price = round(total_amount / total_vol, 2) if total_vol > 0 else 0
                data.avg = avg_price

        # 提交更新
        session.commit()
        print(f"交易日 {trade_date} 的平均成交价计算完成并更新")

# 示例用法
if __name__ == "__main__":
    # 以下为使用示例，您可以替换为实际的股票代码和日期
    # print(get_minute_data('600000.SH', '2020-01-08'))
    # import sys
    # from trade_calendar import TradeCalendar
    # start_date = sys.argv[1]
    # end_date = sys.argv[2]
    # TradeCalendar.iterate_trade_days(start_date, end_date, calculate_avg_price)

    fetch_and_save_data('002866.SZ', '20220630')
