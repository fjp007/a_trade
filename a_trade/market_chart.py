# coding: utf-8
import matplotlib.pyplot as plt
import os
import sys
from a_trade.db_base import Session
from a_trade.market_analysis import MarketDailyData
from a_trade.trade_calendar import TradeCalendar
import datetime
import logging
from a_trade.settings import get_chinese_font, get_project_path

# 获取市场分析数据
def _fetch_market_daily_data(start_date, end_date):
    session = Session()
    try:
        records = session.query(MarketDailyData).filter(
            MarketDailyData.trade_date.between(start_date, end_date)
        ).order_by(MarketDailyData.trade_date.asc()).all()
        return records
    finally:
        session.close()

# 绘制市场指标图表
def plot_market_indicators(start_date, end_date):
    if not TradeCalendar.validate_date_range(start_date, end_date):
        return

    data = _fetch_market_daily_data(start_date, end_date)
    if not data:
        logging.warning(f"{start_date} - {end_date} 没有找到市场分析数据")
        return

    dates = [record.trade_date for record in data]

    # 创建一个包含所有图表的纵向排列图像
    fig, axs = plt.subplots(5, 1, figsize=(12, 35))

    # 绘制 "涨停数量"、"非一字涨停数量"、"跌停数量" 在第一张子图上
    axs[0].plot(dates, [record.limit_up_num for record in data], label="涨停数量", color='r')
    axs[0].plot(dates, [record.non_one_word_limit_up_num for record in data], label="非一字涨停数量", color='y')
    axs[0].plot(dates, [record.limit_down_num for record in data], label="跌停数量", color='g')
    axs[0].set_title("涨停、非一字涨停、跌停数量", fontproperties=get_chinese_font())
    axs[0].set_xlabel("交易日期", fontproperties=get_chinese_font())
    axs[0].set_ylabel("数量", fontproperties=get_chinese_font())
    axs[0].tick_params(axis='x', rotation=45)
    axs[0].legend(prop=get_chinese_font())

    # 绘制 "Rate" 相关指标在第二张子图上
    axs[1].plot(dates, [record.prev_limit_up_high_open_rate for record in data], label="昨日打板高开率", color='b')
    axs[1].plot(dates, [record.prev_limit_up_success_rate for record in data], label="昨日打板成功率", color='g')
    axs[1].plot(dates, [record.blow_up_rate for record in data], label="炸板率", color='r')
    axs[1].plot(dates, [record.physical_board_next_day_limit_up_rate for record in data], label="实体板次日连板率", color='m')
    axs[1].set_title("打板指标", fontproperties=get_chinese_font())
    axs[1].set_xlabel("交易日期", fontproperties=get_chinese_font())
    axs[1].set_ylabel("比例", fontproperties=get_chinese_font())
    axs[1].tick_params(axis='x', rotation=45)
    axs[1].legend(prop=get_chinese_font())

    # 绘制 "打板博弈金额" 的图表在第三张子图上
    values = [round(record.limit_up_amount / 1e8, 1) for record in data]  # 将金额转换为亿元
    axs[2].plot(dates, values, label="打板博弈金额", color='b')
    axs[2].set_title("打板博弈金额", fontproperties=get_chinese_font())
    axs[2].set_xlabel("交易日期", fontproperties=get_chinese_font())
    axs[2].set_ylabel("金额（单位：亿元）", fontproperties=get_chinese_font())
    axs[2].tick_params(axis='x', rotation=45)
    axs[2].legend(prop=get_chinese_font())

    # 绘制 "情绪指标" 的图表在第四张子图上
    sentiment_values = [record.sentiment_index for record in data]
    logging.info(sentiment_values)
    axs[3].plot(dates, sentiment_values, label="情绪指标", color='b')
    axs[3].axhline(y=90, color='r', linestyle='--')
    axs[3].axhline(y=70, color='y', linestyle='--')
    axs[3].axhline(y=50, color='purple', linestyle='--')
    axs[3].axhline(y=30, color='g', linestyle='--')
    axs[3].axhline(y=10, color='black', linestyle='--')
    axs[3].set_title("情绪指标", fontproperties=get_chinese_font())
    axs[3].set_xlabel("交易日期", fontproperties=get_chinese_font())
    axs[3].set_ylabel("情绪指数", fontproperties=get_chinese_font())
    axs[3].tick_params(axis='x', rotation=45)
    axs[3].legend(prop=get_chinese_font())

    # 添加区域标注
    axs[3].text(dates[-1], 95, '沸点', color='r', fontproperties=get_chinese_font())
    axs[3].text(dates[-1], 75, '过热', color='y', fontproperties=get_chinese_font())
    axs[3].text(dates[-1], 55, '微热', color='purple', fontproperties=get_chinese_font())
    axs[3].text(dates[-1], 35, '微冷', color='g', fontproperties=get_chinese_font())
    axs[3].text(dates[-1], 15, '过冷', color='black', fontproperties=get_chinese_font())
    axs[3].text(dates[-1], 5, '冰点', color='black', fontproperties=get_chinese_font())

    # 新增：绘制 "市场最高空间板" 的图表在第五张子图上
    highest_continuous_up_counts = [record.highest_continuous_up_count for record in data]
    axs[4].plot(dates, highest_continuous_up_counts, label="市场最高空间板", color='b')
    axs[4].set_title("市场最高空间板", fontproperties=get_chinese_font())
    axs[4].set_xlabel("交易日期", fontproperties=get_chinese_font())
    axs[4].set_ylabel("板数", fontproperties=get_chinese_font())
    axs[4].tick_params(axis='x', rotation=45)
    axs[4].legend(prop=get_chinese_font())

    plt.tight_layout()

    report_dir = get_project_path() / 'daily_report'
    report_dir.mkdir(parents=True, exist_ok=True)
    file_path = report_dir / f"{end_date}_市场情绪.png"

    plt.savefig(file_path)
    logging.info(f"已保存图像：{file_path}")
    return file_path

# 绘制最近一个月的市场指标图表
def plot_market_indicators_recent_month(end_date):
    # 绘制最近一个月数据
    start_date = (datetime.datetime.strptime(end_date, "%Y%m%d") - datetime.timedelta(days=30)).strftime("%Y%m%d")
    return plot_market_indicators(start_date, end_date)

def main():
    if len(sys.argv) != 2:
        print("用法: python market_chart.py <结束日期(yyyyMMdd)>")
        sys.exit(1)
    end_date = sys.argv[1]
    plot_market_indicators_recent_month(end_date)

if __name__ == "__main__":
    main()