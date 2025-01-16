# coding: utf-8
from sqlalchemy import desc, and_, or_
import logging
import jieba
from a_trade.concept_manager import conceptManager, ConceptInfo
from a_trade.limit_up_data_tushare import LimitUpTushare
from a_trade.concept_relations import conceptRelations
from a_trade.reason_concept import reasonConceptManager
from a_trade.limit_attribution import LimitDailyAttribution
from a_trade.db_base import Session
import json

def find_stock_with_concept(name):
    with Session() as session:
        result = session.query(LimitUpTushare).join(
            LimitDailyAttribution, 
            and_(
                LimitUpTushare.trade_date == LimitDailyAttribution.trade_date, 
                LimitUpTushare.stock_code == LimitDailyAttribution.stock_code
            )
        ).filter(
            LimitDailyAttribution.concept_name == name,
            LimitUpTushare.limit_status == 'U'
        ).order_by(desc(LimitUpTushare.trade_date)).all()
        for record in result:
           logging.info(f"{record.trade_date} {record.stock_name} ({record.stock_code}) {record.reason_type} -{record.last_time} - {record.continuous_limit_up_count}")

def find_remain_concept():
    all_concepts = conceptManager.concept_info_cache.values()
    remain_concepts = []
    # 打开文件并读取数据
    for record in all_concepts:
        if record.name not in conceptRelations.concept_to_category:
            print(record.name)

def find_similar_concept():
    with Session() as session:
        i_concepts = session.query(ConceptInfo).filter(
            ConceptInfo.is_available == 1,
            ConceptInfo.exchange == 'A',
            ConceptInfo.type == 'I',
            ConceptInfo.name.isnot(None)
        ).all()

        n_concepts = session.query(ConceptInfo).filter(
            ConceptInfo.is_available == 1,
            ConceptInfo.exchange == 'A',
            ConceptInfo.type == 'N',
            ConceptInfo.name.isnot(None)
        ).all()

        i_list= [concept.name for concept in i_concepts]
        n_list= [concept.name for concept in n_concepts]

    # 分词并构建词集合的函数
    def get_words_set(name):
        words = jieba.lcut(name)
        return set(words)
    
    ignore_words = set(['设备', '技术', '服务', '概念', '和', '及', ',', '(', ')', "A股","与", "制造", "Ⅲ"])
    # 存储每个名称的分词结果
    words_dict1 = {name: get_words_set(name) for name in i_list}
    words_dict2 = {name: get_words_set(name) for name in n_list}

    # 寻找相似组合
    similar_pairs = []
    # for name1, words1 in words_dict1.items():
    #     for name2, words2 in words_dict2.items():
    #         common_words = words1 & words2
    #         filtered_common_words = common_words - ignore_words
    #         if len(filtered_common_words) >= 1:
    #             similar_pairs.append((name1, name2, filtered_common_words))
    for name1, words1 in words_dict1.items():
        for name2, words2 in words_dict1.items():
            if name1 != name2:
                common_words = words1 & words2
                filtered_common_words = common_words - ignore_words
                if len(filtered_common_words) >= 1:
                    similar_pairs.append((name1, name2, filtered_common_words))

    # 打印结果
    for pair in similar_pairs:
        name1, name2, common_words = pair
        print(f"名称1: {name1}, 名称2: {name2}, 共同词语: {common_words}")

def analyze_limit_up_reasons(start_date, end_date):
        """
        分析指定日期范围内涨停股票的原因统计
        
        参数:
        - start_date: 开始日期
        - end_date: 结束日期
        """
        with Session() as session:
            # 查询涨停股票数据
            limit_up_stocks = session.query(LimitUpTushare).filter(
                LimitUpTushare.trade_date.between(start_date, end_date),
                LimitUpTushare.limit_status == 'U'
            ).all()

            # 初始化按日期分类的统计字典
            trade_date_to_reasons_count = {}

            # 遍历涨停股票
            for stock in limit_up_stocks:
                trade_date = stock.trade_date
                reason_type = stock.reason_type

                if not reason_type:
                    continue
                separators = ['+', '＋', ' ']
                reasons = [reason_type]  # 默认值为整个字符串

                # 根据分隔符分割原因
                for sep in separators:
                    new_reasons = reason_type.split(sep)
                    if len(new_reasons) > 1:
                        reasons = new_reasons
                        break

                # 提取 reasons[0]
                main_reason = reasons[0].strip()  # 提取 reasons[0] 并清理空格
                if not main_reason:
                    continue

                # 初始化当天统计字典
                if trade_date not in trade_date_to_reasons_count:
                    trade_date_to_reasons_count[trade_date] = {}
                
                # 更新当天 reasons[0] 的统计
                reasons_count = trade_date_to_reasons_count[trade_date]
                reasons_count[main_reason] = reasons_count.get(main_reason, 0) + 1

            # 按日期处理统计结果
            for trade_date, reasons_count in trade_date_to_reasons_count.items():
                # 筛选出现次数超过 3 次的 reasons[0]
                frequent_reasons = {reason: count for reason, count in reasons_count.items() if count > 3}

                # 排除不在 concept_info_cache 中的原因
                valid_reasons = {reason: count for reason, count in frequent_reasons.items() 
                                if reason not in conceptManager.concept_info_cache
                                and (reason + '概念') not in conceptManager.concept_info_cache 
                                and reason not in reasonConceptManager.custom_concept_words 
                                and all(word not in reason for word in ['新股', '预增', '季度', '季报', '年报', "基建",
                                                                        "医药", "车联网", "股权转让", "券商", "扭亏为盈", "东数西算",
                                                                        "旅游", "AMC", "跨境支付", "环保", "大消费", "BIPV", "多元金融",
                                                                        "华为海思", "中字头", "其它", "Sora概念", "飞行汽车", "光模块"])
                                }

                # 打印结果
                if valid_reasons:
                    for reason, count in valid_reasons.items():
                        print(f" {trade_date}  {reason}: {count} 次")

def clear_stock_name_in_db():
    from a_trade.limit_attribution import LimitDailyAttribution
    from a_trade.trade_utils import strip_stock_name
    from a_trade.db_base import Session

    with Session() as session:
        try:
            # 查询所有带有空格的股票名称
            stocks_with_spaces = session.query(LimitDailyAttribution).filter(
                LimitDailyAttribution.stock_name.contains(' ')
            ).all()

            # 更新每个带有空格的股票名称
            for stock in stocks_with_spaces:
                new_name = strip_stock_name(stock.stock_name)
                if new_name != stock.stock_name:
                    stock.stock_name = new_name
                    session.merge(stock)
            
            # 提交更改
            session.commit()
        except Exception as e:
            # 发生异常时回滚
            session.rollback()
            logging.error(f"更新股票名称失败: {str(e)}")
            raise

if __name__ == "__main__":
    import sys
    # find_stock_with_concept(sys.argv[1])
    # find_remain_concept()
    # find_similar_concept()
    clear_stock_name_in_db()
    
            
