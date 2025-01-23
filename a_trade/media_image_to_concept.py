# coding: utf-8

import os
import logging
import json
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy import Column, String, Float, Integer, func, and_

from a_trade.db_base import Base, engine, Session, String
from a_trade.media_data_process import WechatLimitArticle, is_black_background
from a_trade.trade_calendar import TradeCalendar
from a_trade.settings import get_project_path
from a_trade.concept_llm_analysis import analyze_concept_datas_from_media_image
from a_trade.limit_attribution import LimitDailyAttribution
from a_trade.trade_utils import code_with_exchange
from a_trade.limit_up_data_tushare import LimitDataSource

class LimitDailyAttributionMedia(Base):
    # 涨停日内归因表
    __tablename__ = 'limit_daily_attribution_media'
    stock_code = Column(String, primary_key=True)
    trade_date = Column(String, primary_key=True)
    stock_name = Column(String, nullable=False)
    concept_name = Column(String, primary_key=True)
    category = Column(String)
Base.metadata.create_all(engine)

def translate_image_to_concept_for_day(trade_date: str) -> None:
    translate_image_to_json_for_day(trade_date)
    traslate_json_to_db(trade_date)

def translate_image_to_json_for_day(trade_date: str) -> None:
    image_dir = os.path.join(get_project_path(), 'media_data', trade_date, 'tables')
    logging.info(f"即将处理图片文件夹 {image_dir}")
    if not os.path.exists(image_dir):
        logging.warning(f"图片文件不存在: {image_dir}")
        return
    
    black_image_path: Optional[str] = None
    json_path = os.path.join(get_project_path(), 'media_data', trade_date, 'daily_concept.json')
    # 如果json文件存在则删除
    if os.path.exists(json_path):
        try:
            os.remove(json_path)
            logging.info(f"已删除旧JSON文件: {json_path}")
        except Exception as e:
            logging.error(f"删除JSON文件失败: {json_path}, 错误: {e}")
            
    # 遍历目录下的所有文件
    # 按文件名中的数字排序处理图片
    files = [f for f in os.listdir(image_dir) if f.startswith('table_') and f.endswith('.png')]
    # 提取文件名中的数字并排序
    files.sort(key=lambda x: x.split('_')[1].split('.')[0].zfill(2))
    
    # 跳过最后两张图片
    if len(files) > 2:
        files = files[:-2]
    
    for filename in files:
        file_path = os.path.join(image_dir, filename)
        try:
            logging.info(f"正在处理图片: {file_path}")
            if black_image_path:
                # 黑色背景子图之后才是真正的表格图片,利用视觉理解模型提取表格数据至JSON
                json_data = analyze_concept_datas_from_media_image(file_path)
                
                # 处理JSON文件合并
                existing_data: Dict[str, Any] = {}
                if os.path.exists(json_path):
                    try:
                        with open(json_path, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                    except json.JSONDecodeError:
                        logging.warning(f"JSON文件格式错误，将覆盖: {json_path}")
                        existing_data = {}
                
                # 合并新数据，一级分类采用覆盖逻辑
                if isinstance(json_data, list):
                    # 如果是列表，遍历每个字典并合并
                    for data_dict in json_data:
                        for category, concepts in data_dict.items():
                            # 直接覆盖一级分类下的所有数据
                            existing_data[category] = concepts
                else:
                    # 如果是字典，直接覆盖一级分类下的所有数据
                    for category, concepts in json_data.items():
                        existing_data[category] = concepts
                
                # 写入更新后的数据
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(existing_data, f, ensure_ascii=False, indent=2)
                logging.info(f"成功更新概念数据到: {json_path}")

            if is_black_background(file_path):
                black_image_path = file_path
        except Exception as e:
            logging.error(f"处理图片失败: {file_path}, 错误: {e}")

def traslate_json_to_db(trade_date: str) -> None:
    """
    JSON文件数据结构如下
    {
"一级分类名":{
"概念1":{
"股票代码1": "股票名称1",
"股票代码2": "股票名称2"
},
"概念2":{
"股票代码3": "股票名称3",
"股票代码4": "股票名称4"
}
}
}
    """
    json_path = os.path.join(get_project_path(), 'media_data', trade_date, 'daily_concept.json')
    if not os.path.exists(json_path):
        logging.warning(f"JSON文件不存在: {json_path}")
        return

    session = Session()
    ignore_category = ['非一字连板', '非新股一字板', '大长腿', '含可转债涨停']
    try:
        # 读取JSON文件
        with open(json_path, 'r', encoding='utf-8') as f:
            concept_data: Dict[str, Any] = json.load(f)
        # 将数据转换为LimitDailyAttributionMedia对象
        media_objects: List[LimitDailyAttributionMedia] = []
        # 查询当日所有归因数据
        existing_attributions = session.query(LimitDailyAttribution).filter(
            LimitDailyAttribution.trade_date == trade_date
        ).all()

        # 构建股票归因旧数据映射，stock_name到(stock_code, concept_names)的映射
        stock_concept_map: Dict[str, Dict[str, Any]] = {}
        for attribution in existing_attributions:
            if attribution.stock_name not in stock_concept_map:
                stock_concept_map[attribution.stock_name] = {
                    'stock_code': attribution.stock_code,
                    'concept_names': []
                }
            if attribution.concept_name not in stock_concept_map[attribution.stock_name]['concept_names']:
                stock_concept_map[attribution.stock_name]['concept_names'].append(attribution.concept_name)

        today_limit_source = LimitDataSource(trade_date)
        name_code_map = today_limit_source.get_name_to_code()
        def name_to_code(stock_name: str, stock_code: Optional[str]) -> str:
            if stock_name in name_code_map:
                return name_code_map[stock_name]
            else:
                if stock_code and len(stock_code) == 6:
                    code = code_with_exchange(stock_code)
                    return code
                assert False, f" {stock_name} {stock_code} 非法"

        # 构建股票归因新数据映射
        new_stock_concept_map: Dict[str, Dict[str, Any]] = {}
        # 遍历一级分类
        for category, concepts in concept_data.items():
            # 如果category包含ignore_category中的任意元素，则跳过
            if any(ignore_cat in category for ignore_cat in ignore_category):
                continue
            logging.info(f"处理一级分类 {category}")
            # 遍历每个概念
            for concept_name, stocks in concepts.items():
                logging.info(f"处理概念 {concept_name}")
                if '涨停被砸' not in category:
                    for code, stock_name in stocks.items():
                        # 更新新数据映射
                        stock_code =  name_to_code(stock_name, code)
                        if stock_name not in new_stock_concept_map:
                            new_stock_concept_map[stock_name] = {
                                'stock_code': stock_code,
                                'concept_names': []
                            }
                        if concept_name not in new_stock_concept_map[stock_name]['concept_names']:
                            new_stock_concept_map[stock_name]['concept_names'].append(concept_name)
                            
                        media_obj = LimitDailyAttributionMedia(
                            trade_date=trade_date,
                            concept_name=concept_name,
                            category=category,
                            stock_code=stock_code,
                            stock_name=stock_name
                        )
                        media_objects.append(media_obj)
                else:
                    for stock_name, _ in stocks.items():
                        # 更新新数据映射
                        stock_code =  name_to_code(stock_name, None)
                        # 处理概念名称，如果是"-"分割的，取最后一个元素
                        if '-' in concept_name:
                            split_concept_name = concept_name.split('-')[-1].strip()
                        else:
                            split_concept_name = concept_name
                            
                        if stock_name not in new_stock_concept_map:
                            new_stock_concept_map[stock_name] = {
                                'stock_code': stock_code,
                                'concept_names': []
                            }
                        if split_concept_name not in new_stock_concept_map[stock_name]['concept_names']:
                            new_stock_concept_map[stock_name]['concept_names'].append(split_concept_name)
                            
                        media_obj = LimitDailyAttributionMedia(
                            trade_date=trade_date,
                            concept_name=split_concept_name,
                            category=category,
                            stock_code=stock_code,
                            stock_name=stock_name
                        )
                        media_objects.append(media_obj)

        # 检查是否有旧数据里的股票但未被新数据归因
        missing_stocks: List[str] = []
        for stock_name in stock_concept_map:
            if stock_name not in new_stock_concept_map:
                missing_stocks.append(stock_name)
        
        if missing_stocks:
            logging.error(f"以下股票在旧数据中存在但未被新数据归因: {missing_stocks}")
            session.rollback()
            session.close()
            raise ValueError(f"存在未归因的股票: {missing_stocks}")

        # 批量写入数据库
        session.bulk_save_objects(media_objects)
        session.commit()
        logging.info(f"成功将{trade_date}的概念数据写入数据库，共{len(media_objects)}条记录")
        
    except Exception as e:
        session.rollback()
        logging.error(f"写入概念数据失败: {e}")
    finally:
        session.close()

def translate_image_to_concept_during(start_date: str, end_date: str) -> None:
    with Session() as session:
        try:
            # 删除指定日期范围内的数据
            deleted_count = session.query(LimitDailyAttributionMedia).filter(
                LimitDailyAttributionMedia.trade_date >= start_date,
                LimitDailyAttributionMedia.trade_date <= end_date
            ).delete()
            
            session.commit()
            logging.info(f"成功清理{start_date}至{end_date}的LimitDailyAttributionMedia数据，共删除{deleted_count}条记录")
        except Exception as e:
            session.rollback()
            logging.error(f"清理LimitDailyAttributionMedia数据失败: {e}")
    TradeCalendar.iterate_trade_days(start_date, end_date, translate_image_to_concept_for_day)

if __name__ == '__main__':
    import sys
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    translate_image_to_concept_during(start_date, end_date)