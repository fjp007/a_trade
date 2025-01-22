# coding: utf-8
import os
import datetime
import sys
import logging

from a_trade.trade_calendar import TradeCalendar
from a_trade.stocks_daily_data import update_stocks_daily_data_until
from a_trade.index_daily_data import update_index_data_until
from a_trade.limit_up_data_tushare import update_limit_up_data_until, fetch_reason_types_from_wencai_util, process_frequent_reasons_during
from a_trade.market_analysis import update_market_daily_data_during
from a_trade.market_chart import plot_market_indicators_recent_month
from a_trade.wechat_bot import WechatBot
from a_trade.concept_manager import conceptManager
from a_trade.stock_base import update_new_stocks
import a_trade.settings
from a_trade.limit_attribution import update_limit_daily_attribution_during
from a_trade.concept_daily_chart import plot_limit_up_concepts_chart
from a_trade.strategy_Yugi_s1 import StrategyYugiS1
from a_trade.db_base import merge_db_data_from_sync_to_base, merge_db_data_from_base_to_sync

env = os.getenv('ENV')
if env == 'dev':
    env = 'test'
    os.environ['ENV'] = 'test'  # 设置测试环境

def main(today=None):
    # 获取今天日期
    if not today:
        today = datetime.datetime.now().strftime("%Y%m%d")

    if env == 'release':
        merge_db_data_from_sync_to_base()
    
    # 判断今天是否是交易日
    trade_date_result = TradeCalendar.is_trade_day(today)
    if trade_date_result is None:
        logging.error(f"{today} 未能成功更新至交易日历")
        sys.exit(0)

    if trade_date_result == False:
        logging.info(f"{today} 不是交易日，停止执行脚本。")
        sys.exit(0)
    
    # 获取今日所有股票交易数据并插入数据库
    update_stocks_daily_data_until(today)
    update_new_stocks(today)
    
    # 获取今天的指数数据并插入数据库
    update_index_data_until(today)
    
    # 获取今日涨跌停数据并插入数据库
    limit_date_start_date = update_limit_up_data_until(today)
    
    # 分析市场情绪
    update_market_daily_data_during(limit_date_start_date, today)

    # 更新板块数据
    new_concept_infos = conceptManager.update_concept_info()
    if new_concept_infos:
        WechatBot.send_text_msg(f"同花顺发布新指数: {','.join(new_concept_infos)}")
    conceptManager.update_concept_stock_relation()
    conceptManager.update_concept_daily_data_until(today)      # 更新板块当前交易日日线数据

    # 分析涨停归因数据
    last_full_empty_date = fetch_reason_types_from_wencai_util(today)
    if last_full_empty_date:
        process_frequent_reasons_during(last_full_empty_date, today)
        update_limit_daily_attribution_during(last_full_empty_date, today)  # 涨停数据日内归因

    logging.getLogger('matplotlib').setLevel(logging.WARNING)

    # 生成复盘日报图片
    logging.info(f"正在生成 {today} 的复盘日报图片")
    daily_report_send_failed = True
    market_report_path = plot_market_indicators_recent_month(today)
    if market_report_path and os.path.exists(market_report_path):
        response = WechatBot.send_image_msg(market_report_path)
        daily_report_send_failed = (response == False)
    if daily_report_send_failed:
        WechatBot.send_text_msg("今日复盘日报发送失败")

    # concept_report_path = plot_limit_up_concepts_chart(today)
    # if concept_report_path and os.path.exists(concept_report_path):
    #     response = send_image_msg(market_report_path)
    #     daily_report_send_failed = (response == False)
    # if daily_report_send_failed:
    #     send_msg('text', "今日复盘日报发送失败")

    if env != 'release':
        start_date = last_full_empty_date if last_full_empty_date else today
        strategy = StrategyYugiS1()
        def run_strategy(trade_date):
            task = strategy.generate_daily_task(trade_date)
            task.trade_did_end()
            if trade_date == today:
                StrategyYugiS1().generate_daily_task(today).daily_report()
        TradeCalendar.iterate_trade_days(start_date, today, run_strategy)

    if env == 'test':
        merge_db_data_from_base_to_sync()

if __name__ == "__main__":
    main()
    sys.exit(0)