# coding: utf-8

import os
import logging
import json
from sqlalchemy import Column, String, Float, Integer, func, and_

from a_trade.db_base import Base, engine, Session
from a_trade.media_data_process import WechatLimitArticle, is_black_background
from a_trade.trade_calendar import TradeCalendar
from a_trade.settings import get_project_path
from a_trade.concept_llm_analysis import analyze_concept_datas_from_media_image
from a_trade.limit_attribution import LimitDailyAttribution
from a_trade.trade_utils import code_with_exchange

class LimitDailyAttributionMedia(Base):
    # 涨停日内归因表
    __tablename__ = 'limit_daily_attribution_media'
    stock_code = Column(String, primary_key=True)
    trade_date = Column(String, primary_key=True)
    stock_name = Column(String, nullable=False)
    concept_name = Column(String, primary_key=True)
    category = Column(String)

def translate_image_to_concept_for_day(trade_date):
    image_dir = os.path.join(get_project_path(), 'media_data', trade_date, 'tables')
    logging.info(f"即将处理图片文件夹 {image_dir}")
    if not os.path.exists(image_dir):
        logging.warning(f"图片文件不存在: {image_dir}")
        return
    
    black_image_path = None
    json_path = os.path.join(get_project_path(), 'media_data', trade_date, 'daily_concept.json')
    # 遍历目录下的所有文件
    # 按文件名中的数字排序处理图片
    files = [f for f in os.listdir(image_dir) if f.startswith('table_') and f.endswith('.png')]
    # 提取文件名中的数字并排序
    files.sort(key=lambda x: x.split('_')[1].split('.')[0].zfill(2))
    
    for filename in files:
        file_path = os.path.join(image_dir, filename)
        try:
            logging.info(f"正在处理图片: {file_path}")
            if black_image_path:
                # 黑色背景子图之后才是真正的表格图片,利用视觉理解模型提取表格数据至JSON
                json_data = analyze_concept_datas_from_media_image(file_path)
                
                # 处理JSON文件合并
                existing_data = []
                if os.path.exists(json_path):
                    try:
                        with open(json_path, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                    except json.JSONDecodeError:
                        logging.warning(f"JSON文件格式错误，将覆盖: {json_path}")
                        existing_data = []
                
                # 合并新数据
                if isinstance(json_data, list):
                    existing_data.extend(json_data)
                else:
                    existing_data.append(json_data)
                
                # 写入更新后的数据
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(existing_data, f, ensure_ascii=False, indent=2)
                logging.info(f"成功更新概念数据到: {json_path}")

            if is_black_background(file_path):
                black_image_path = file_path
        except Exception as e:
            logging.error(f"处理图片失败: {file_path}, 错误: {e}")

    traslate_json_to_db(trade_date)

def traslate_json_to_db(trade_date):
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
            concept_data = json.load(f)

        # 将数据转换为LimitDailyAttributionMedia对象
        media_objects = []
        
        # 遍历一级分类
        for category, concepts in concept_data.items():
            # 如果category包含ignore_category中的任意元素，则跳过
            if any(ignore_cat in category for ignore_cat in ignore_category):
                continue
                
            # 遍历每个概念
            for concept_name, stocks in concepts.items():
                # 创建媒体对象
                if '涨停被砸' not in concept_name:
                    for stock_code, stock_name in stocks.items():
                        media_obj = LimitDailyAttributionMedia(
                            trade_date=trade_date,
                            concept_name=concept_name,
                            category=category,
                            stock_code=code_with_exchange(stock_code),
                            stock_name=stock_name
                        )
                        media_objects.append(media_obj)
                else:
                    for stock_name, _ in stocks.items():
                        media_obj = LimitDailyAttributionMedia(
                            trade_date=trade_date,
                            concept_name=concept_name,
                            category=category,
                            stock_code=stock_code,
                            stock_name=stock_name
                        )
                        media_objects.append(media_obj)

        # 批量写入数据库
        session.bulk_save_objects(media_objects)
        session.commit()
        logging.info(f"成功将{trade_date}的概念数据写入数据库，共{len(media_objects)}条记录")
        
    except Exception as e:
        session.rollback()
        logging.error(f"写入概念数据失败: {e}")
    finally:
        session.close()

def translate_image_to_concept_during(start_date, end_date):
    TradeCalendar.iterate_trade_days(start_date, end_date, translate_image_to_concept_for_day)

if __name__ == '__main__':
    import sys
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    print(start_date)
    translate_image_to_concept_during(start_date, end_date)