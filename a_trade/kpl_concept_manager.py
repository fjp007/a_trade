# coding: utf-8

import logging
from sqlalchemy import Column, String, Float
from a_trade.db_base import Session, Base, engine
from a_trade.settings import _get_tushare
import pandas as pd
from a_trade.trade_calendar import TradeCalendar

class KPLConceptDailyAttribution(Base):
    # 涨停日内归因表
    __tablename__ = 'kpl_concept_daily_attribution'
    stock_code = Column(String, primary_key=True)
    trade_date = Column(String, primary_key=True)
    stock_name = Column(String, nullable=False)
    concept_code = Column(String, primary_key=True)
    concept_name = Column(String, primary_key=True)
    description = Column(String)
    hot_num = Column(Float)

def fetch_and_store_concept_data_during(start_date, end_date):
    TradeCalendar.iterate_trade_days(start_date, end_date, fetch_and_store_concept_data_for_day)

def fetch_and_store_concept_data_for_day(trade_date: str):
    """
    拉取指定交易日的开盘啦概念题材成分股数据并写入数据库。

    :param trade_date: 交易日期（YYYYMMDD）
    """

    # 创建数据库表（如果不存在）
    Base.metadata.create_all(engine)

    # 获取Tushare API对象
    pro = _get_tushare()

    # 创建会话
    session = Session()

    logging.info(f"更新交易日开盘啦概念映射数据：{trade_date}")

    # 从API获取数据
    try:
        df = pro.pro_api().kpl_concept_cons(trade_date=trade_date)
    except Exception as e:
        logging.error(f"从Tushare API获取数据时出错（日期：{trade_date}）：{e}")
        session.close()
        return

    # 检查DataFrame是否为空
    if df.empty:
        logging.info(f"交易日期 {trade_date} 无返回数据")
        session.close()
        return
    print(df)

    # 遍历DataFrame并将记录插入数据库
    records = []
    for _, row in df.iterrows():
        record = KPLConceptDailyAttribution(
            stock_code=row['cons_code'],
            trade_date=row['trade_date'],
            stock_name=row['cons_name'],
            concept_code=row['ts_code'],
            concept_name=row['name'],
            description=row['desc'],
            hot_num=row['hot_num']
        )
        records.append(record)

    # 将记录添加到会话中
    try:
        session.bulk_save_objects(records)
        session.commit()
        logging.info(f"已向数据库插入 {len(records)} 条记录（日期：{trade_date}）。")
    except Exception as e:
        logging.error(f"向数据库插入记录时出错（日期：{trade_date}）：{e}")
        session.rollback()
    finally:
        session.close()
        logging.info(f"交易日期 {trade_date} 数据处理完成。")

if __name__ == "__main__":
    # 示例调用
    import sys
    # fetch_and_store_concept_data_during(sys.argv[1], sys.argv[2])

    df = _get_tushare().pro_api().kpl_concept_cons(trade_date=sys.argv[1], ts_code="000279.KP")
    for _, row in df.iterrows():
        print(row)
    # print(df)
