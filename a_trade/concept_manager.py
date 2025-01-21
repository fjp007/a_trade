# coding: utf-8
import logging
import datetime
from sqlalchemy import Column, String, Integer, Float, Table, ForeignKey, or_ , and_, func
from a_trade.db_base import Session, Base, engine
from a_trade.trade_calendar import TradeCalendar
from a_trade.settings import _get_tushare
import pandas as pd
import json

class ConceptInfo(Base):
    __tablename__ = 'concept_info'
    concept_code = Column(String, primary_key=True)
    name = Column(String)
    count = Column(Integer)
    exchange = Column(String)
    list_date = Column(String)
    type = Column(String)
    is_available = Column(Integer, nullable=False)
    daily_data_start_date = Column(String)
    daily_data_end_date = Column(String)

class ConceptStockRelation(Base):
    __tablename__ = 'concept_stock_relation'
    concept_code = Column(String, primary_key=True)
    stock_code = Column(Integer, primary_key=True)
    concept_name = Column(String, nullable=True)
    stock_name = Column(String, nullable=True)

class ConceptDailyData(Base):
    __tablename__ = 'concepts_daily_data'
    concept_code = Column(String, primary_key=True)
    trade_date = Column(String, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    pre_close = Column(Float)
    change = Column(Float)
    pct_chg = Column(Float)
    vol = Column(Float)
    amount = Column(Float)
    turnover_rate = Column(Float)
    avg_price = Column(Float)

target_conditions = [
    (ConceptInfo.is_available == 1),
    (ConceptInfo.exchange == 'A'),
    or_(
        ConceptInfo.type.in_(['N', 'I']),
        and_(
            ConceptInfo.type == 'S',
            ConceptInfo.name == '破净股'
        )
    ),
    ConceptInfo.name.isnot(None)
]

class ConceptManager:
    def __init__(self):
        self.concept_info_cache = {}
        self._load_concept_infos()

    def _load_concept_infos(self):
        # 加载板块信息至内存缓存
        result = self._get_all_available_subject_concept()
        for record in result:
            self.concept_info_cache[record.name] = record

    def get_code_from_concept_name(self, name):
        if name not in self.concept_info_cache:
            return None
        concept = self.concept_info_cache[name]
        return concept.concept_code

    def _get_all_available_subject_concept(self):
        with Session() as session:
            subject_concepts = session.query(ConceptInfo).filter(*target_conditions).all()
        return subject_concepts

    def update_concept_info(self):
        session = Session()
        try:
            request_types = {
                'I': '行业指数',
                'N': '概念指数',
                # 'S': '同花顺特色指数'
            }
            new_concept_list = []
            for request_type, desc in request_types.items():
                logging.info(f"正在更新 {desc}")
                concept_data = _get_tushare().pro_api().ths_index(type=request_type, exchange='A')
                response_codes = concept_data['ts_code']

                local_data = session.query(ConceptInfo).filter(
                    ConceptInfo.concept_code.in_(response_codes),
                    ConceptInfo.type == request_type
                ).all()
                
                # 将查询结果转换为概念代码集
                existing_codes = {item.concept_code for item in local_data}
                new_concepts = concept_data[~concept_data['ts_code'].isin(existing_codes)]
                
                # 记录并插入新概念
                for index, row in new_concepts.iterrows():
                    new_concept = ConceptInfo(
                        concept_code=row['ts_code'],
                        name=row['name'],
                        count=int(row['count']) if not pd.isna(row['count']) else None,
                        exchange=row['exchange'],
                        list_date=row['list_date'],
                        type=row['type'],
                        is_available= 1 if not pd.isna(row['count']) else 0,
                        daily_data_start_date=None,
                        daily_data_end_date=None
                    )
                    session.add(new_concept)
                
                session.commit()
                new_concept_list += new_concepts['name'].tolist()
                # new_concept_list += list(new_concepts.loc[:, ['name']].itertuples(index=False, name=None))
            
            return new_concept_list
        except Exception as e:
            session.rollback()
            logging.error(f"请求接口ths_index 新增板块数据发生错误: {e}")
            return []
        finally:
            session.close()

    def update_concept_stock_relation(self):
        with Session() as session:
            # 查询 ConceptInfo 表中满足条件且在 ConceptStockRelation 表中没有对应记录的概念代码
            subquery = session.query(ConceptStockRelation.concept_code).distinct().subquery()
            results = session.query(ConceptInfo).outerjoin(
                subquery, ConceptInfo.concept_code == subquery.c.concept_code
            ).filter(
                *target_conditions,
                subquery.c.concept_code.is_(None)
            ).all()
            if results:
                # 在这里进行你想要的处理，例如打印输出或更新
                for concept in results:
                    logging.info(f"板块: {concept.name}({concept.concept_code}) 缺少映射信息，即将请求")
                    ths_member_result = _get_tushare().pro_api().ths_member(ts_code=concept.concept_code)
                    print(ths_member_result)
                    for _, row in ths_member_result.iterrows():
                        new_relation = ConceptStockRelation(
                            concept_code=concept.concept_code,
                            concept_name=concept.name,
                            stock_code=row['ts_code'],
                            stock_name=row['con_name']
                        )
                        session.merge(new_relation)
                session.commit()
            else:
                logging.info("没有要更新的板块映射关系")

    # 更新题材+行业板块日线数据截止至end_date
    def update_concept_daily_data_until(self, end_date):
        session = Session()
        try:
            concepts = self.concept_info_cache.values()
            concept_codes = [concept.concept_code for concept in concepts]
            request_start_date = {}
            for concept_info in concepts:
                concept_code = concept_info.concept_code
                last_trade_date = concept_info.daily_data_end_date

                if last_trade_date:
                    next_trade_date =  TradeCalendar.get_next_trade_date(last_trade_date)
                    if next_trade_date is not None:
                        start_date = next_trade_date
                    else:
                        continue
                else:
                    start_date ='20140101'

                if start_date > end_date:
                    continue
                if start_date not in request_start_date:
                    request_start_date[start_date] = []
                
                request_start_date[start_date].append(concept_code)

            for start_date, concept_codes in request_start_date.items():
                daily_data = _get_tushare().pro_api().ths_daily(ts_code=','.join(concept_codes), start_date=start_date, end_date=end_date)
                if (len(daily_data) == 0):
                    logging.info(f"板块{concept_codes} 自{start_date}后不再有日线数据")
                else:
                    logging.info(f"从{start_date} - {end_date} 请求了{len(concept_codes)}个题材日线数据, 返回{len(daily_data)} 个题材日线数据")
                for _, row in daily_data.iterrows():
                    amount = row['avg_price'] * row['vol'] if row.get('avg_price') is not None and row.get('vol') is not None else None
                    new_data = ConceptDailyData(
                        concept_code=row['ts_code'],
                        trade_date=row['trade_date'],
                        open=row.get('open', None),
                        high=row.get('high', None),
                        low=row.get('low', None),
                        close=row.get('close', None),
                        pre_close=row.get('pre_close', None),
                        change=row.get('change', None),
                        pct_chg=row.get('pct_change', None),
                        vol=row.get('vol', None),
                        amount=amount,
                        turnover_rate=row.get('turnover_rate', None),
                        avg_price=row.get('avg_price', None)
                    )
                    session.merge(new_data)
                    if any(row.get(field, None) is None for field in ['open', 'high', 'low', 'close', 'pre_close', 'change', 'vol']):
                        logging.error(f"数据缺失，跳过插入: {row.to_dict()}")
                session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"更新题材日线数据失败: {e}")
        finally:
            session.close()

        self._update_concept_trade_date()
    
    # 更新题材+行业板块有效交易日期
    def _update_concept_trade_date(self):
        session = Session()
        try:
            concepts = session.query(ConceptInfo).all()
            for concept in concepts:
                min_date, max_date = session.query(
                    func.min(ConceptDailyData.trade_date),
                    func.max(ConceptDailyData.trade_date)
                ).filter(ConceptDailyData.concept_code == concept.concept_code).one_or_none()
            # logging.debug(f"{concept.name}:日线表最早交易日{min_date}，最晚交易日{max_date}")
                if min_date is not None:
                    concept.daily_data_start_date = min_date
                if max_date is not None:
                    concept.daily_data_end_date = max_date
                session.merge(concept)
            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"更新题材交易日期失败: {e}")
        finally:
            session.close()

    # 查找在指定日期缺失的题材板块数据
    def find_missing_concepts_for_date(self, trade_date):
        session = Session()
        try:
            requested_codes = {
                concept.concept_code for concept in self.concept_info_cache.values()
            }

            retrieved_codes = {
                data.concept_code for data in session.query(ConceptDailyData.concept_code).filter(
                    ConceptDailyData.trade_date == trade_date
                ).all()
            }

            missing_concepts = requested_codes - retrieved_codes

            if missing_concepts:
                logging.info(f"在日期 {trade_date} concept_info表中的板块没有日线交易数据：")
                for concept_code in missing_concepts:
                    concept = session.query(ConceptInfo).filter(ConceptInfo.concept_code == concept_code).first()
                    if concept:
                        logging.info(f"板块代码: {concept_code}, 板块名称: {concept.name}")
        except Exception as e:
            logging.error(f"查找缺失题材板块数据失败: {e}")
        finally:
            session.close()

    def find_concepts_with_missing_data(self):
        session = Session()
        try:
            # Fetching the list of available concepts
            concepts = self.concept_info_cache.values()
            missing_data_concepts = []

            for concept in concepts:
                # Fetching the earliest and latest trade dates for the concept
                start_date = concept.daily_data_start_date
                end_date = concept.daily_data_end_date

                if start_date and end_date:
                    # Fetching all trade dates for the concept between start_date and end_date
                    trade_dates = session.query(ConceptDailyData.trade_date).filter(
                        ConceptDailyData.concept_code == concept.concept_code,
                        ConceptDailyData.trade_date.between(start_date, end_date)
                    ).all()

                    # Converting trade dates to a set for easy comparison
                    trade_dates_set = set(date[0] for date in trade_dates)

                    # Generating all possible dates between start_date and end_date
                    all_dates = TradeCalendar.get_trade_dates(start_date, end_date)

                    # Finding missing dates
                    missing_dates = [date for date in all_dates if date not in trade_dates_set]

                    if len(missing_dates) > 50:
                        missing_data_concepts.append((concept.concept_code, concept.name, missing_dates))

            # Printing concepts with missing data
            for concept_code, concept_name, missing_dates in missing_data_concepts:
                logging.info(f"Concept '{concept_name}' (Code: {concept_code}) 中间缺失日线数据超过50天")
                concept = session.query(ConceptInfo).filter(ConceptInfo.concept_code == concept_code).first()
                if concept:
                    concept.is_available = 0
            session.commit()
        except Exception as e:
            logging.error(f"板块日线数据清洗失败 {e}")
        finally:
            session.close()

    def check_daily_data(self):
        session = Session()
        try:
            result = session.query(ConceptDailyData).filter(
                ConceptDailyData.pct_chg == None
            ).all()
            for data in result:
                trade_date = data.trade_date
                pre_trade_date = TradeCalendar.get_previous_trade_date(trade_date)
                pre_trade_data = session.query(ConceptDailyData).filter(
                    ConceptDailyData.trade_date == pre_trade_date,
                    ConceptDailyData.concept_code == data.concept_code
                ).first()
                if pre_trade_data and pre_trade_data.close is not None and data.close:
                    if not data.pre_close:
                        data.pre_close = pre_trade_data.close
                        if not data.change:
                            data.change = data.close - pre_trade_data.close
                            if not data.pct_chg:
                                if pre_trade_data.close != 0:
                                    data.pct_chg = round((data.change / pre_trade_data.close * 100), 4)  # 四舍五入保留四位小数
                                else:
                                    data.pct_chg = 0

                    session.commit()  # 提交更新
                    logging.info(f"{data.trade_date} - {data.concept_code} 数据已更新")
                else:
                    logging.info(f"{data.trade_date} - {data.concept_code} 没有前一天的数据，无法更新")

        except Exception as e:
            logging.error(f"板块日线数据清洗失败 {e}")
        finally:
            session.close()
            
conceptManager = ConceptManager()

if __name__ == "__main__":
    conceptManager.check_daily_data()

