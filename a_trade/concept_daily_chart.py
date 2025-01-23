# coding: utf-8
#concept_daily_chart.py

import pandas as pd
import matplotlib.pyplot as plt
import os
from a_trade.db_base import Session
from a_trade.limit_attribution import LimitDailyAttribution
from a_trade.limit_up_data_tushare import LimitUpTushare, LimitDataSource
from a_trade.settings import get_chinese_font, get_project_path
import logging
from a_trade.trade_utils import format_time_string, format_amount_string, format_limit_up_count_string
from a_trade.market_chart import plot_market_indicators_recent_month

def plot_limit_up_concepts_chart(trade_date: str) -> str:
    concept_to_stock_list = {}
    concept_to_data = {}

    with Session() as session:
        # 查询指定日期下所有涨停的题材信息
        stock_concept_result = session.query(
            LimitDailyAttribution.concept_name,
            LimitDailyAttribution.concept_code,
            LimitDailyAttribution.stock_code 
        ).filter(
            LimitDailyAttribution.trade_date == trade_date
        ).distinct().all()
        
        limit_datasource = LimitDataSource(trade_date)
        for stock_concept_relation in stock_concept_result:
            concept_code = stock_concept_relation.concept_code
            concept_name = stock_concept_relation.concept_name
            stock_code = stock_concept_relation.stock_code
            if concept_code:
                if concept_name not in concept_to_stock_list:
                    concept_to_stock_list[concept_name] = []
                    concept_to_data[concept_name] = (0, 0)
                if stock_code in limit_datasource.limit_up_map:
                    stock_info = limit_datasource.limit_up_map[stock_code]

                if stock_info:
                    stock_list = concept_to_stock_list[concept_name]
                    stock_list.append((concept_name, 
                                       stock_code,
                                       stock_info.stock_name,
                                       stock_info.pct_chg,
                                       stock_info.close,
                                       stock_info.float_mv,
                                       stock_info.amount,
                                       stock_info.continuous_limit_up_count,
                                       stock_info.last_time,
                                       stock_info.reason_type,
                                       stock_info.open_times,
                                       stock_info.up_stat))
                    limit_count, concept_limit_amount = concept_to_data[concept_name]
                    limit_count += 1
                    concept_limit_amount += stock_info.amount
                    concept_to_data[concept_name] = (limit_count, concept_limit_amount)
                else:
                    logging.error(f"{trade_date} {stock_code} 没有找到涨停板数据")
    delete_concept_names = []
    for concept_name, stock_list in concept_to_stock_list.items():
        if not stock_list or (len(stock_list)<2 and stock_list[0][7] < 2):
            delete_concept_names.append(concept_name)
    for key in delete_concept_names:
        del concept_to_stock_list[key]
        del concept_to_data[key]
    if not concept_to_data:
        logging.error("没有题材数据")
        return None

    # 转换 concept_to_data 到 result_data_summary
    result_data_summary = [
        (concept_name, data[0], data[1])
        for concept_name, data in concept_to_data.items()
    ]
    # 按成交金额排序，这里假设金额已经是数值类型
    result_data_summary.sort(key=lambda x: x[2], reverse=True)  # 从大到小排序

    # 按照 result_data_summary 的顺序重排 concept_to_stock_list
    sorted_concept_to_stock_list = {}
    for concept_name, _, _ in result_data_summary:
        sorted_concept_to_stock_list[concept_name] = concept_to_stock_list[concept_name]
    # 对每个 stock_list 按连板数进行排序
    for stock_list in sorted_concept_to_stock_list.values():
        stock_list.sort(key=lambda x: (-x[7], x[8]))  # x[7] 是连板数，按连板数从大到小排序; x[6] 是成交额，按封板时间排序
    
    # 将汇总结果转换为 Pandas DataFrame
    df_summary = pd.DataFrame(result_data_summary, columns=['题材名称', '涨停数', '涨停成交总额'])
    df_summary['涨停成交总额'] = df_summary['涨停成交总额'].apply(format_amount_string)

    logging.getLogger('matplotlib').setLevel(logging.WARNING)

    # 计算图形布局参数
    row_height = 0.5
    total_row_count = len(df_summary)*2 + 1 + sum(len(data) for _, data in sorted_concept_to_stock_list.items())
    figure_height = total_row_count * row_height
    relative_row_height = 1 / total_row_count
    relative_row_height = int(relative_row_height * 10000) / 10000

    # 创建具有计算高度的图形
    fig = plt.figure(figsize=(16, figure_height), dpi=100)
    logging.info(f"Total rows: {total_row_count}, Figure height: {figure_height}, Relative row height: {relative_row_height}")

    # 设置字体以支持中文显示
    prop = get_chinese_font()

    current_height = 1.0  # 跟踪每个表格的当前高度位置

    # 绘制汇总表格
    summary_height = (1 +len(df_summary)) * relative_row_height
    if current_height - summary_height >= 0:
        bottom_position = current_height - summary_height

        ax_summary = fig.add_axes([0.05, bottom_position, 0.9, summary_height])
        ax_summary.axis('tight')
        ax_summary.axis('off')
        table = ax_summary.table(cellText=df_summary.values, colLabels=df_summary.columns, cellLoc='center', loc='center', bbox=[0.1, 0.05, 0.8, 0.9])
        logging.debug(f"Summary Table - Rows: {len(df_summary)+1}, Bottom Position: {bottom_position}, Height: {summary_height}")
        for key, cell in table.get_celld().items():
            cell.set_text_props(fontproperties=prop)
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.auto_set_column_width(col=list(range(len(df_summary.columns))))

        current_height = bottom_position  # 更新当前高度

    # 绘制每个题材的详细表格
    for concept_code, stock_list in sorted_concept_to_stock_list.items():
        df = pd.DataFrame(stock_list, columns=['题材名称', "代码", "名字", "涨幅(%)", "涨停价", "流通市值", "成交额", "连板数", "最后封板时间", "涨停原因", "涨停开板次数", "板/天"])
        # 检查 DataFrame 是否为空
        if df.empty:
            logging.warning(f"{concept_name} 没有数据")
            continue

        df['最后封板时间'] = df['最后封板时间'].apply(format_time_string)
        df['成交额'] = df['成交额'].apply(format_amount_string)
        df['流通市值'] = df['流通市值'].apply(format_amount_string)
        df['板/天'] = df['板/天'].apply(format_limit_up_count_string)
        # format_limit_up_count_string
        detail_height = (len(df)+1) * relative_row_height
        if current_height - detail_height >= 0:
            bottom_position = current_height - detail_height
            
            ax_detail = fig.add_axes([0.02, bottom_position, 0.96, detail_height])

            logging.debug(f"Detailed Table ({concept_name}) - Rows: {len(df)+1}, Bottom Position: {bottom_position}, Height: {detail_height}")

            ax_detail.axis('tight')
            ax_detail.axis('off')
            
            table = ax_detail.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center', bbox=[0.1, 0.05, 0.8, 0.9])
            # 合并第一列的单元格
            if len(df) > 1:
             # 遍历每一行
                for i in range(len(df)+1):
                    if i == 1:
                        # 第一行的第一列，下方边框不展示
                        table[(i, 0)].visible_edges = 'LTR'
                    elif i == len(df):
                        # 最后一行的第一列，上方边框不展示
                        table[(i, 0)].visible_edges = 'LRB'
                    elif i > 1:
                        # 其余行的第一列，上下边框都不展示
                        table[(i, 0)].visible_edges = 'LR'

                    # 除了第一行，其余行的第一列数据不展示
                    if i > 1:
                        table[(i, 0)].get_text().set_text('')

            for key, cell in table.get_celld().items():
                cell.set_text_props(fontproperties=prop)
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            table.auto_set_column_width(col=list(range(len(df.columns))))

            current_height = bottom_position  # 更新当前高度
        else:
            logging.warning(f"Not enough space for detailed table of {concept_name}, skipping...")

    # 将图形保存到 daily_report 目录下
    if not os.path.exists('daily_report'):
        os.makedirs('daily_report')

    report_dir = get_project_path() / 'daily_report'
    report_dir.mkdir(parents=True, exist_ok=True)
    output_path = report_dir / f"{trade_date}_题材分析.png"

    logging.info(f"Figure size (inches): {fig.get_size_inches()} Figure DPI: {fig.dpi} Figure size (pixels): {fig.get_size_inches() * fig.dpi}")
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()
    return output_path

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("用法: python concepts_daily_data.py <绘制日期(yyyyMMdd)>")
        sys.exit(1)
    trade_date = sys.argv[1]
    plot_limit_up_concepts_chart(trade_date)
    plot_market_indicators_recent_month(trade_date)
