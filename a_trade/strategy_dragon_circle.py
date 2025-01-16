# coding: utf-8
from a_trade.trade_calendar import TradeCalendar
from a_trade.xlsx_file_manager import XLSXFileManager
from a_trade.db_base import Session
from sqlalchemy import func, desc
from a_trade.limit_up_data_tushare import LimitUpTushare


def get_highest_space_mapping(trade_days):
    """
    获取初始的交易日到最高空间股票和最高空间板数的映射。
    :param session: 数据库会话
    :param trade_days: 交易日列表
    :return: 初始映射 dict，格式为 {trade_date: (最高空间股票数组, 最高连板数)}
    """
    with Session() as session:
        mapping = {}
        for trade_date in trade_days:
            results = session.query(LimitUpTushare.stock_name, LimitUpTushare.continuous_limit_up_count) \
                .filter(LimitUpTushare.trade_date == trade_date) \
                .order_by(desc(LimitUpTushare.continuous_limit_up_count)) \
                .all()
            if results:
                max_limit_up = results[0][1]
                highest_stocks = [stock_name for stock_name, count in results if count == max_limit_up]
                mapping[trade_date] = (highest_stocks, max_limit_up)
        return mapping


def filter_highest_space_mapping(mapping, trade_days):
    """
    倒序处理映射数据，筛选符合规则的最高空间股票和板数。
    :param mapping: 初始映射数据
    :param trade_days: 交易日列表
    :return: 筛选后的映射数据
    """
    filtered_mapping = mapping.copy()

    # 倒序遍历交易日
    for i in range(len(trade_days) - 1, 0, -1):
        current_date = trade_days[i]
        previous_date = trade_days[i - 1]

        current_stocks, _ = filtered_mapping.get(current_date, ([], None))
        previous_stocks, previous_count = filtered_mapping.get(previous_date, ([], None))

        # 如果前一日股票不在下一日股票中，说明断板，需要剔除
        valid_previous_stocks = [stock for stock in previous_stocks if stock in current_stocks]
        if not valid_previous_stocks:
            valid_previous_stocks = previous_stocks
        else:
            filtered_mapping[previous_date] = (valid_previous_stocks, previous_count)

    return filtered_mapping


def export_dragon_cycle_to_excel(start_date, end_date):
    """
    导出处理后的最高空间板龙头周期表到 Excel。
    :param start_date: 开始日期，格式为 'YYYYMMDD'
    :param end_date: 结束日期，格式为 'YYYYMMDD'
    """
    # 初始化 Excel 文件管理器
    trade_record_path = f"/strategy_result/历史高标龙头股周期表{start_date}_{end_date}.xlsx"
    trade_file_manager = XLSXFileManager(trade_record_path, "龙头周期",
                                         ["交易日期", "当前最高空间板股票", "最高空间板", "下一周期最高空间股票", "下一周期股票当前连板数"],
                                         [0], True)

    # 获取交易日列表并初始映射
    trade_days = TradeCalendar.get_trade_dates(start_date, end_date)
    initial_mapping = get_highest_space_mapping(trade_days)

    # 筛选映射数据
    filtered_mapping = filter_highest_space_mapping(initial_mapping, trade_days)
    with Session() as session:
        # 倒序生成 Excel 数据
        next_highest_stock  = []
        for trade_date in reversed(trade_days):
            if trade_date in filtered_mapping:
                current_highest_stock, current_highest_count = filtered_mapping[trade_date]
                if next_highest_stock:
                    next_highest_info = session.query(LimitUpTushare).filter(
                        LimitUpTushare.trade_date ==trade_date,
                        LimitUpTushare.stock_name == next_highest_stock[0]
                    ).first()
                    next_highest_count = next_highest_info.continuous_limit_up_count if next_highest_info else 0
                else:
                    next_highest_count = 0
                trade_file_manager.insert_and_save([
                    trade_date,
                    ','.join(current_highest_stock),
                    current_highest_count,
                    ','.join(next_highest_stock),
                    next_highest_count
                ])
                # 更新下一周期数据
                next_highest_stock = current_highest_stock

    print(f"数据已成功导出到 {trade_record_path}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("用法: python strategy_dragon_circle.py <开始日期(yyyyMMdd)> <结束日期(yyyyMMdd)>")
        sys.exit(1)

    start_date = sys.argv[1]
    end_date = sys.argv[2]

    export_dragon_cycle_to_excel(start_date, end_date)
