# coding: utf-8
# limit_attribution.py

import logging
import datetime
from sqlalchemy import Column, String, Float, Integer, func, and_
from sqlalchemy.sql import exists
from a_trade.db_base import engine, Base, Session, get_recent_trade_date_in_table
from a_trade.trade_calendar import TradeCalendar
from a_trade.limit_up_data_tushare import LimitUpTushare, LimitDataSource
from a_trade.concept_manager import ConceptStockRelation, ConceptInfo, conceptManager, ConceptDailyData
from a_trade.stock_base import StockBase
from a_trade.xlsx_file_manager import XLSXFileManager
import json
from a_trade.concept_relations import conceptRelations
from a_trade.reason_concept import reasonConceptManager
from a_trade.trade_utils import strip_list, time_difference_less_than
from a_trade.settings import get_project_path

valid_time_interval_concept_effect = 30 * 60

# 定义数据库模型
class ConceptDailyAnalysis(Base):
    # 题材日内表现表
    __tablename__ = 'concept_daily_analysis'
    concept_code = Column(String, primary_key=True)
    trade_date = Column(String, primary_key=True)
    concept_name = Column(String, nullable=False)
    concept_pch_chg = Column(Float, nullable=False)
    limit_up_count = Column(Integer, nullable=False)
    dragon_first_stock_code = Column(String)
    dragon_first_stock_name = Column(String)
    dragon_second_stock_code = Column(String)
    dragon_second_stock_name = Column(String)

class LimitDailyAttribution(Base):
    # 涨停日内归因表
    __tablename__ = 'limit_daily_attribution'
    stock_code = Column(String, primary_key=True)
    trade_date = Column(String, primary_key=True)
    stock_name = Column(String, nullable=False)
    concept_code = Column(String)
    concept_name = Column(String, primary_key=True)

file_manager = None

def update_limit_daily_attribution_util(end_date):
    start_date = get_recent_trade_date_in_table(ConceptDailyAnalysis.__tablename__)
    update_limit_daily_attribution_during(start_date, end_date)
    
# 更新涨停股票的行业和题材信息
def update_limit_daily_attribution_during(start_date, end_date):
    if not TradeCalendar.validate_date_range(start_date, end_date):
        return
    
    # 清理指定日期范围内的归因数据
    _clear_limit_daily_attribution_in_range(start_date, end_date)
    
    # _update_concept_analysis_data(start_date, end_date)
    global file_manager
    file_path = get_project_path() / '归因失败股票.xlsx'
    file_manager = XLSXFileManager(file_path, "失败归因股票", ["交易日", "股票代码", "股票名称", "涨停原因"], [0, 1], True)
    TradeCalendar.iterate_trade_days(start_date, end_date,_update_limit_daily_attribution_for_day)
    _update_failed_limit_daily_attribution_during(start_date, end_date)

def _clear_limit_daily_attribution_in_range(start_date, end_date):
    """
    清理 LimitDailyAttribution 表中指定日期范围内的数据
    """
    with Session() as session:
        session.query(LimitDailyAttribution).filter(
            and_(
                LimitDailyAttribution.trade_date >= start_date,
                LimitDailyAttribution.trade_date <= end_date
            )
        ).delete(synchronize_session=False)
        session.commit()

def _update_concept_analysis_data(start_date, end_date):
    concepts = conceptManager.concept_info_cache
    session = Session()
    try:
        for _, concept in concepts.items():
            logging.info(f"更新板块{concept.name} {start_date} - {end_date} 日内分析数据")
            # 一次性获取所有日期的数据
            concept_data = session.query(ConceptDailyData).filter(
                ConceptDailyData.concept_code == concept.concept_code,
                ConceptDailyData.trade_date.between(start_date, end_date)
            ).all()

            concept_updates = {}
            for data in concept_data:
                if data.pct_chg is not None:
                    if data.trade_date not in concept_updates:
                        concept_updates[data.trade_date] = data

            # 更新或插入新记录
            for trade_date, daily_data in concept_updates.items():
                analysis_entry = session.merge(ConceptDailyAnalysis(
                    concept_code=concept.concept_code,
                    trade_date=trade_date,
                    concept_name=concept.name,
                    concept_pch_chg=daily_data.pct_chg,
                    limit_up_count=0  # Placeholder; will update later
                ))
        session.commit()
    except Exception as e:
        logging.error(f"更新板块 {trade_date} 日内分析数据失败: {e}")
        session.rollback()
    finally:
        session.close()

def _append_stock_to_concept(concept_to_unique_stocks, concept_name, stock_code):
    if concept_name in concept_to_unique_stocks and stock_code:
        if stock_code not in concept_to_unique_stocks[concept_name]:
            stock_list = concept_to_unique_stocks[concept_name]
            stock_list.append(stock_code)
    else:
        concept_to_unique_stocks[concept_name] = []
        concept_to_unique_stocks[concept_name].append(stock_code)

def _update_failed_limit_daily_attribution_during(start_date, end_date):
    TradeCalendar.iterate_trade_days(start_date, end_date, _update_failed_limit_daily_attribution_for_day)

def _update_failed_limit_daily_attribution_for_day(trade_date):
    # 获取炸板数据
    session = Session()
    stock_to_last_concept = {}
    limit_data_source = LimitDataSource(trade_date)
    failed_limit_stock_codes = [ stock.stock_code for stock in limit_data_source.limit_failed_map.values()]

    # 获取最近一周的涨停归因数据
    last_week_date = TradeCalendar.get_previous_trade_date(trade_date, 5)
    last_week_limit_attributions = session.query(LimitDailyAttribution)\
        .join(LimitUpTushare, LimitUpTushare.stock_code == LimitDailyAttribution.stock_code)\
        .filter(LimitDailyAttribution.trade_date.between(last_week_date, trade_date),
                LimitUpTushare.limit_status == 'U',
                LimitUpTushare.stock_code.in_(failed_limit_stock_codes))\
        .all()
    # 构建股票对应的最新板块归因
    for limit_attribution in last_week_limit_attributions:
        stock_name = limit_attribution.stock_name
        if stock_name not in stock_to_last_concept:
            stock_to_last_concept[stock_name] = limit_attribution
        else:
            origin_limit_attribution = stock_to_last_concept[stock_name]
            if limit_attribution.trade_date > origin_limit_attribution.trade_date:
                stock_to_last_concept[stock_name] = limit_attribution

    # 获取当日涨停涨停股票代码
    limit_up_stock_codes = [stock.stock_code for stock in limit_data_source.limit_up_map.values()]

    # 从 LimitDailyAttribution 表中获取板块-股票映射
    concept_to_stocks = {}
    if limit_up_stock_codes:
        limit_up_attributions = session.query(LimitDailyAttribution).filter(
            LimitDailyAttribution.trade_date == trade_date,
            LimitDailyAttribution.stock_code.in_(limit_up_stock_codes)
        ).all()

        for attribution in limit_up_attributions:
            concept_name = attribution.concept_name
            stock_code = attribution.stock_code
            if concept_name not in concept_to_stocks:
                concept_to_stocks[concept_name] = []
            concept_to_stocks[concept_name].append(stock_code)
    
    try:
    # 更新至数据库
        for _, limit_attribution in stock_to_last_concept.items():
            ori_concept_name = limit_attribution.concept_name
            new_concept_name = ori_concept_name
            chain = conceptRelations.concept_to_chains.get(ori_concept_name, [])
            for parent_concept in chain:
                if parent_concept in concept_to_stocks and len(concept_to_stocks[parent_concept]) >= 2:
                    new_concept_name = parent_concept
                    logging.info(f"炸板股票 {trade_date} {limit_attribution.stock_name} 板块归因为在 {ori_concept_name} 链条上找到具有板块效应的板块 {parent_concept}")
                    break
            logging.info(f"炸板股票 {trade_date} {limit_attribution.stock_name} 板块归因为: {new_concept_name}")

            # 更新LimitDailyAttribution表
            attribution_data = LimitDailyAttribution(
                stock_code=limit_attribution.stock_code,
                trade_date=trade_date,
                stock_name=limit_attribution.stock_name,
                concept_code=limit_attribution.concept_code,
                concept_name=new_concept_name
            )
            session.merge(attribution_data)
        session.commit()

    except Exception as e:
        session.rollback()
        logging.error(f"更新炸板股票题材数据失败: {e}")
    finally:
        session.close()

def verify_concept_effect_for_limit_time(concept_to_stocks, limit_data_source: LimitDataSource, stock_code_to_name):
    """
    处理板块归因数据。
    
    参数：
    - concept_to_stocks: dict, 键为板块名称，值为股票代码列表。
    - limit_up_map: dict, 键为股票代码，值为 LimitUpTushare 对象。

    返回：
    - dict, 处理后的 concept_to_stocks。
    """
    # Step 1: 构建股票代码到板块的映射
    limit_up_map = limit_data_source.limit_up_map
    stock_to_concepts = {}
    for concept, stocks in concept_to_stocks.items():
        for stock_code in stocks:
            stock_to_concepts.setdefault(stock_code, []).append(concept)
    
    # 筛选出关联多个板块的股票代码
    multi_concept_stocks = {
        stock_code: sorted(
            concepts,
            key=lambda concept: len(concept_to_stocks.get(concept, []))
        )
        for stock_code, concepts in stock_to_concepts.items()
        if len(concepts) > 1
    }

    # Step 2: 检查板块有效性
    for stock_code, concepts in multi_concept_stocks.items():
        max_length = max(len(concept_to_stocks[concept]) for concept in concepts if concept in concept_to_stocks)
        limit_data = limit_up_map.get(stock_code)

        # 获取当前股票的首次涨停时间
        stock_limit_up_first_time = limit_data.first_time
        stock_limit_up_last_time = limit_data.last_time
        
        for concept in concepts:
            if max_length - len(concept_to_stocks[concept]) > 3:
                concept_to_stocks[concept].remove(stock_code)
                stock_to_concepts[stock_code].remove(concept)
                logging.info(f"移除小板块归因：股票 {stock_code_to_name[stock_code]} 从板块 {concept} 移除, 剩余板块 {stock_to_concepts[stock_code]} 最大长度{max_length}")
                continue
            
            valid = False  # 标记当前板块是否有效归因
            logging.info(f"检验板块内股票首次涨停时间 {limit_data_source.trade_date} {concept} {stock_code_to_name[stock_code]} {stock_limit_up_first_time} {stock_limit_up_last_time}")
            # 遍历板块中的其他股票
            for other_stock_code in concept_to_stocks[concept]:
                if other_stock_code == stock_code or other_stock_code not in limit_up_map:
                    continue  # 跳过自身或无涨停信息的股票
                
                other_limit_up_time = limit_up_map[other_stock_code].first_time
                
                logging.info(f"检验板块内其它股票首次涨停时间 {limit_data_source.trade_date} {concept} {stock_code_to_name[other_stock_code]} {limit_up_map[other_stock_code].first_time} {limit_up_map[other_stock_code].last_time}")  
                # 计算涨停时间间隔          
                if not valid and (time_difference_less_than(stock_limit_up_first_time, other_limit_up_time, valid_time_interval_concept_effect) or time_difference_less_than(stock_limit_up_last_time, limit_up_map[other_stock_code].last_time, valid_time_interval_concept_effect)):  # 时间间隔小于30分钟
                    valid = True
            
            # 如果板块无效归因，移除该股票
            if not valid:
                if len(stock_to_concepts[stock_code]) > 1:
                    concept_to_stocks[concept].remove(stock_code)
                    stock_to_concepts[stock_code].remove(concept)
                    logging.info(f"移除无效归因：股票 {stock_code_to_name[stock_code]} 从板块 {concept} 移除, 剩余板块 {stock_to_concepts[stock_code]}")
                else:
                    logging.warning(f"股票 {stock_code_to_name[stock_code]} 需要检查归因 {concept}: {limit_up_map[stock_code].reason_type}")
    
    multi_concept_stocks = {
        stock_code: sorted(
            concepts,
            key=lambda concept: len(concept_to_stocks.get(concept, []))
        )
        for stock_code, concepts in stock_to_concepts.items()
        if len(concepts) > 1
    }
    for stock_code, concepts in multi_concept_stocks.items():
        max_length = max(len(concept_to_stocks[concept]) for concept in concepts if concept in concept_to_stocks)
        limit_data = limit_up_map.get(stock_code)

        for concept in concepts:
            if max_length - len(concept_to_stocks[concept]) > 1:
                concept_to_stocks[concept].remove(stock_code)
                stock_to_concepts[stock_code].remove(concept)
                logging.info(f"移除小板块归因：股票 {stock_code_to_name[stock_code]} 从板块 {concept} 移除, 剩余板块 {stock_to_concepts[stock_code]} 最大长度{max_length}")
                continue

        if max_length == 1:
            reason_type = limit_data.reason_type

            def find_first_available_concept(reason_type):
                # 获取涨停原因类别
                separators = ['+', '＋', ' ']
                reasons = [reason_type]  # 默认值为整个字符串
                for sep in separators:
                    new_reasons = reason_type.split(sep)
                    if len(new_reasons) > 1:
                        reasons = new_reasons
                        break
                
                for reason in reasons:
                    limit_stock_concept_names = reasonConceptManager.find_valid_related_concept(reason)
                    if limit_stock_concept_names:
                        return limit_stock_concept_names
                
                return None
            
            first_available_concept = find_first_available_concept(reason_type)[0]
            for concept in concepts:
                if not concept == first_available_concept and len(stock_to_concepts[stock_code]) > 1 and stock_code in concept_to_stocks[concept]:
                    logging.info(f"移除单一股票非首涨停原因归因 {stock_code_to_name[stock_code]} 从板块 {concept} 移除, 剩余板块 {stock_to_concepts[stock_code]}")
                    concept_to_stocks[concept].remove(stock_code)
                    stock_to_concepts[stock_code].remove(concept)
                    
            continue
        
    # 返回处理后的 concept_to_stocks
    return concept_to_stocks

def _clean_concept_to_stocks(concept_to_stocks, stock_code_to_name, clean_line=3,):
    """
    移除 concept_to_stocks 中没有板块效应的股票，并更新字典。
    :param concept_to_stocks: dict，键为概念名称，值为股票代码的数组
    :return: dict，处理后的 concept_to_stocks
    """
    logging.info(f"删除板块<{clean_line}的无效归因")
    # Step 1: 找出所有有板块效应的概念（股票数量 >= clean_line）
    concepts_with_effect = {concept for concept, stocks in concept_to_stocks.items() if len(stocks) >= clean_line}
    
    # Step 2: 汇总所有有板块效应的股票代码
    stocks_with_concept_effect = set()
    for concept in concepts_with_effect:
        stocks_with_concept_effect.update(concept_to_stocks[concept])
    
    # Step 3: 遍历 concept_to_stocks，移除无效股票
    for concept in list(concept_to_stocks.keys()):  # 使用 list(concept_to_stocks.keys()) 防止迭代时修改
        if concept not in concepts_with_effect:  # 无板块效应的概念
            removed_stocks = [
                stock_code_to_name[stock_code] for stock_code in concept_to_stocks[concept]
                if stock_code in stocks_with_concept_effect
            ]
            if removed_stocks:
                logging.info(f"从概念 {concept} 中移除以下股票: {removed_stocks}")
            
            # 更新板块中的股票，仅保留未出现在其他有效板块中的股票
            concept_to_stocks[concept] = [
                stock for stock in concept_to_stocks[concept]
                if stock not in stocks_with_concept_effect
            ]
        
        # 移除概念内空的记录
        if not concept_to_stocks[concept]:
            del concept_to_stocks[concept]
    
    return concept_to_stocks

def _sum_up_concept_stocks(concept_to_stocks, stock_code_to_name):
    """
    归并板块股票数据（先预处理，将股票逐层累加到父级板块）
    :param concept_to_stocks: 每个板块的股票映射
    :param stock_code_to_name: 股票代码到名称的映射
    :return: 最终的板块股票数据
    """
    # 保存初始状态
    result_concept_to_stocks = concept_to_stocks.copy()  # 返回结果
    sum_up_initial_concepts = []
    remain_target_concept_list = []
    # 遍历每个板块，仅处理股票数量小于 3 的板块
    for concept_name, stocks in concept_to_stocks.items():
        if len(stocks) >= 3:
            has_related_to_clear = False
            for temp_concept in remain_target_concept_list.copy():
                if conceptRelations.is_parent_concept(temp_concept, concept_name):
                    remain_target_concept_list.remove(temp_concept)
                    remain_target_concept_list.append(concept_name)
                    has_related_to_clear = True
                    break
                if conceptRelations.is_parent_concept(concept_name, temp_concept):
                    has_related_to_clear = True
                    break
            if not has_related_to_clear:
                remain_target_concept_list.append(concept_name)
        else:
            # 获取板块的链条
            chain = conceptRelations.concept_to_chains.get(concept_name, [])
            if chain:
                sum_up_initial_concepts.append(concept_name)

            # 逐层暂时累加股票，直到找到满足条件的板块
            for parent_concept in chain:
                if parent_concept not in result_concept_to_stocks:
                    result_concept_to_stocks[parent_concept] = []

                # 暂时累加股票
                temp_stocks = result_concept_to_stocks[parent_concept] + stocks
                temp_stocks = strip_list(temp_stocks)
                result_concept_to_stocks[parent_concept] = temp_stocks

                # 判断是否满足板块效应条件
                if stocks and len(temp_stocks) >= 3:
                    logging.info(f"板块 {concept_name} 的股票 {[stock_code_to_name[code] for code in stocks]} 被归并到满足条件的父级板块 {parent_concept}")
                    remain_target_concept_list.append(parent_concept)
                    # 退出当前链条的处理
                    break

    remain_target_concept_list = strip_list(remain_target_concept_list)
    if remain_target_concept_list:
        logging.info(f"{remain_target_concept_list} 满足板块效应，清洗链上同名股票")
        for clear_concept_name in remain_target_concept_list:
            logging.info(f"清洗板块 {clear_concept_name} 链上同名股票")
            # 获取满足条件板块的相关链条
            related_concept_list = conceptRelations.get_related_concepts(clear_concept_name)

            # 清理链条上其他板块中的股票关系
            for related_concept in related_concept_list:
                if related_concept != clear_concept_name and related_concept in result_concept_to_stocks:
                    # 找出需要清理的股票
                    stocks_to_remove = [
                        stock_code_to_name[stock] for stock in result_concept_to_stocks[related_concept] if stock in result_concept_to_stocks[clear_concept_name]
                    ]
                    # 添加日志输出
                    if stocks_to_remove:
                        logging.info(
                            f"清理板块 {related_concept} 中的股票: {stocks_to_remove}，因为它们已归属于满足条件的板块 {clear_concept_name}"
                        )
                    result_concept_to_stocks[related_concept] = [
                        stock for stock in result_concept_to_stocks[related_concept] if stock not in result_concept_to_stocks[clear_concept_name]
                    ]
                    logging.info(f"清理后 {related_concept} 中的股票: {result_concept_to_stocks[related_concept]}")
    return result_concept_to_stocks


# 对涨停股票进行题材日内归因
def _update_limit_daily_attribution_for_day(trade_date):
    logging.info(f"开始对{trade_date}涨停股票日内归因")
    session = Session()

    # 获取涨停股票
    limit_data_source = LimitDataSource(trade_date)
    limit_up_stocks = limit_data_source.limit_up_map.values()

    stock_code_to_name = {}
    concept_to_stocks = {}  # 题材板块名称到今日涨停股票
    stocks_with_effect = set()
    logging.info(f"{trade_date} 第一次归类开始: 优先处理有涨停原因AI参考的股票")
    i = 0
    while True:
        has_available_reason = False
        logging.info(f"取第 {i+1} 涨停原因分析板块效应")
        for stock in limit_up_stocks:
            stock_code = stock.stock_code
            stock_name = stock.stock_name
            reason_type = stock.reason_type

            stock_code_to_name[stock_code] = stock_name

            if not reason_type:
                continue

            # 获取涨停原因类别
            separators = ['+', '＋', ' ']
            reasons = [reason_type]  # 默认值为整个字符串
            for sep in separators:
                new_reasons = reason_type.split(sep)
                if len(new_reasons) > 1:
                    reasons = new_reasons
                    break
                
            reason = reasons[i] if i < len(reasons) else None
            if reason is not None and has_available_reason == False:
                has_available_reason = True
            if not reason:
                continue
            limit_stock_concept_names = reasonConceptManager.find_valid_related_concept(reason)

            if not limit_stock_concept_names:
                continue

            logging.info(f"交易日{trade_date} {stock_name} ({stock_code}) 通过第 {i+1} 涨停原因({reason_type}) AI归因获得有效归因参考板块: {limit_stock_concept_names}")

            find_continue = (i <=1 or stock_code not in stocks_with_effect)
            # 更新字典2: 记录题材板块到今日涨停股票数的映射
            for concept_name in limit_stock_concept_names:
                if concept_name in concept_to_stocks:
                    if stock_code not in concept_to_stocks[concept_name]:
                        if not find_continue:
                            logging.info(f"{concept_name} 已归因股票数 {len(concept_to_stocks[concept_name])}")
                            if len(concept_to_stocks[concept_name]) >= 5:
                                logging.info(f"{concept_name} 有强烈板块效应")
                                concept_to_stocks[concept_name].append(stock_code)
                            else:
                                logging.info(f"{trade_date} {stock_code_to_name[stock_code]} 已找到板块效应,不再分析第 {i+1} 涨停原因{reason_type}")
                        else:
                            concept_to_stocks[concept_name].append(stock_code)
                elif find_continue:
                    concept_to_stocks[concept_name] = [stock_code]

                if not find_continue and concept_name in conceptRelations.concept_to_chains:
                    parents = conceptRelations.concept_to_chains[concept_name]
                    for name in parents:
                        if name in concept_to_stocks and len(concept_to_stocks[name]) >= 5:
                            max_effect_concept = name
                            logging.info(f"{concept_name} 父板块 {max_effect_concept} 有强烈板块效应")
                            concept_to_stocks[max_effect_concept].append(stock_code)
                            break

        if has_available_reason == False:
            break

        if i > 0:
            concept_to_stocks = _sum_up_concept_stocks(concept_to_stocks, stock_code_to_name)

        # 找出所有有板块效应的概念（股票数量 >= 3）
        concepts_with_effect = {concept for concept, stocks in concept_to_stocks.items() if len(stocks) >= 3}
        
        # 汇总所有有板块效应的股票代码
        for concept in concepts_with_effect:
            stocks_with_effect.update(concept_to_stocks[concept])
    
        i += 1

    concept_to_stocks = verify_concept_effect_for_limit_time(concept_to_stocks, limit_data_source, stock_code_to_name)
    concept_to_stocks = _clean_concept_to_stocks(concept_to_stocks, stock_code_to_name, 2)
    concept_to_stocks = _clean_concept_to_stocks(concept_to_stocks, stock_code_to_name, 3)

    # 使用sorted函数进行排序，基于每个键的值的长度，并设置reverse=True进行降序排序
    sorted_concept_to_stocks = {
        k: v for k, v in sorted(
            concept_to_stocks.items(),
            key=lambda item: len(item[1]),
            reverse=True
        )
    }

    for concept in concept_to_stocks:
        stocks_with_effect.update(concept_to_stocks[concept])
    
    all_stock_code_set = set(stock_code_to_name.keys())
    stocks_with_no_effect = all_stock_code_set - stocks_with_effect
    if stocks_with_no_effect:
        for stock_code in stocks_with_no_effect:
            limit_data = limit_data_source.limit_up_map[stock_code]
            logging.warning(f"第一次AI归因失败: 交易日{trade_date} {stock_code} ({stock_code_to_name[stock_code]}) 无有效板块归因，涨停原因: {limit_data.reason_type}")

            if not limit_data.reason_type:
                 # 查找stock_base表，没有找到的话是ST股票，无需warning
                stock_exist = session.query(
                    session.query(StockBase).filter(
                        StockBase.ts_code == stock_code
                    ).exists()
                ).scalar()
                if stock_exist:
                    logging.error(f"交易日{trade_date} 非ST股票{stock_code} ({stock_name}) 缺少涨停原因数据")
                    file_manager.insert_and_save([trade_date, stock_code, stock_code_to_name[stock_code], limit_data.reason_type])
                    # 将归因失败的股票写入xlsx表格
            else:
                if trade_date != '20240208' and trade_date != '20240930' and trade_date != '20241008': 
                    file_manager.insert_and_save([trade_date, stock_code, stock_code_to_name[stock_code], limit_data.reason_type])

    logging.info(f"{trade_date} 第二次归类开始: 将归因AI无法推导出涨停板块的股票 按照已归类的板块内涨停数量排序，依次添加真实存在映射关系的股票到板块里。")
    #第二次循环，按照已归类的板块内涨停数量排序，依次添加真实存在映射关系的股票到板块里。
    for stock_code, stock_name in stock_code_to_name.items():      
        if stock_code not in stocks_with_effect:
            stock_sorted = Float
            for key in sorted_concept_to_stocks.keys():
                # 尚未分类
                if len(sorted_concept_to_stocks[key]) >=2:
                    limit_data = limit_data_source.limit_up_map[stock_code]
                    if any((time_difference_less_than(limit_data_source.limit_up_map[other_stock_code].first_time,limit_data.first_time, valid_time_interval_concept_effect) or
                            time_difference_less_than(limit_data_source.limit_up_map[other_stock_code].last_time,limit_data.last_time, valid_time_interval_concept_effect)) for other_stock_code in sorted_concept_to_stocks[key]):
                        mapping_exists = session.query(
                            session.query(ConceptStockRelation)
                            .join(ConceptInfo, ConceptStockRelation.concept_code == ConceptInfo.concept_code)
                            .filter(ConceptStockRelation.stock_code == stock_code, ConceptInfo.name == key)
                            .exists()
                        ).scalar()
                        if mapping_exists:
                            logging.info(f"{stock_name} 在第二次归类中被归为 {key} 板块已归因股票数{len(sorted_concept_to_stocks[key])}")
                            _append_stock_to_concept(concept_to_stocks, key, stock_code)
                            stock_sorted = True
                            break
            if not stock_sorted:
                logging.info(f"{stock_name} 在第二次归类中被归为 其它")
                _append_stock_to_concept(concept_to_stocks, '其它', stock_code)

    try:
       # 更新至数据库
        sorted_concept_to_stocks = {
            k: v for k, v in sorted(
                concept_to_stocks.items(),
                key=lambda item: len(item[1]),
                reverse=True
            )
        }
        write_set = set()
        for concept_name, stock_code_list in sorted_concept_to_stocks.items():
            for stock_code in stock_code_list:
                # if stock_code in write_set:
                #     continue
                # write_set.add(stock_code)

                stock_name = stock_code_to_name[stock_code]
                limit_data = limit_data_source.limit_up_map[stock_code]
                logging.info(f"{trade_date} {stock_name} 板块归因为: {concept_name} 涨停原因 {limit_data.reason_type}")
                # 更新LimitDailyAttribution表
                attribution_data = LimitDailyAttribution(
                    stock_code=stock_code,
                    trade_date=trade_date,
                    stock_name=stock_name,
                    concept_code=conceptManager.get_code_from_concept_name(concept_name),
                    concept_name=concept_name
                )
                session.merge(attribution_data)
        session.commit()

    except Exception as e:
        session.rollback()
        logging.error(f"更新涨停股票题材数据失败: {e}")
    finally:
        session.close()

def find_data(start_date, end_date):
    with Session() as session:
        # 查询当天归因到多个板块的股票
        subquery = (
            session.query(
                LimitDailyAttribution.stock_code,
                LimitDailyAttribution.trade_date
            )
            .filter(LimitDailyAttribution.trade_date.between(start_date, end_date))
            .group_by(LimitDailyAttribution.stock_code, LimitDailyAttribution.trade_date)
            .having(func.count(LimitDailyAttribution.concept_name) > 1)
            .subquery()
        )

        # 获取满足条件的股票及其对应的板块信息
        multiple_concept_stocks = (
            session.query(LimitDailyAttribution)
            .join(
                subquery,
                (LimitDailyAttribution.stock_code == subquery.c.stock_code) &
                (LimitDailyAttribution.trade_date == subquery.c.trade_date)
            )
            .order_by(LimitDailyAttribution.trade_date)
            .all()
        )

        # 打印每个股票对应的板块信息
        print("满足条件的股票和板块信息：")
        for record in multiple_concept_stocks:
            print(f"\n交易日: {record.trade_date} 股票名称: {record.stock_name}, 板块名称: {record.concept_name}")
            print("板块内所有股票的详情：")

            concept_stocks = (
                session.query(LimitUpTushare)
                .filter(
                    LimitUpTushare.trade_date == record.trade_date,
                    LimitUpTushare.limit_status != 'D',
                    LimitUpTushare.stock_code.in_(
                        session.query(LimitDailyAttribution.stock_code)
                        .filter(LimitDailyAttribution.concept_name == record.concept_name, LimitDailyAttribution.trade_date == record.trade_date)
                    )
                )
                .all()
            )

            print(f"板块名称: {record.concept_name}")
            if len(concept_stocks) > 1:
                for concept_stock in concept_stocks:
                    print(
                        f"股票名称: {concept_stock.stock_name}, "
                        f"最早涨停时间: {concept_stock.first_time}, "
                        f"最后涨停时间: {concept_stock.last_time}, "
                        f"涨停原因: {concept_stock.reason_type}"
                    )

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("用法: python stock_to_concept.py <交易日(yyyyMMdd)> <交易日(yyyyMMdd)>")
        sys.exit(1)
    start_date = sys.argv[1]
    end_date = sys.argv[2]

    update_limit_daily_attribution_during(start_date, end_date)
    # find_data(start_date, end_date)
    

