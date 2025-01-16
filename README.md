### concept_daily_analysis - 题材日内数据分析表

此表记录每日题材的市场表现，包括涨停数量和龙头股信息。

| 字段名                     | 类型     | 描述                                         |
|----------------------------|----------|----------------------------------------------|
| `concept_code`             | String   | 题材编码，用于唯一标识一个题材，**主键**。      |
| `trade_date`               | String   | 交易日期，用于标识数据属于哪一天，**主键**。    |
| `concept_name`             | String   | 题材名称，用于显示题材的具体名称。             |
| `concept_pch_chg`          | Float    | 当日题材的百分比变化，显示题材日内的表现。     |
| `limit_up_count`           | Integer  | 当日该题材下涨停股票的数量，反映题材的强势。   |
| `dragon_first_stock_code`  | String   | 当日该题材下涨停的龙头股票代码。               |
| `dragon_first_stock_name`  | String   | 龙头股票名称，显示涨停龙头的名称。             |
| `dragon_second_stock_code` | String   | 次龙头股票代码，显示次级涨停股票的代码。       |
| `dragon_second_stock_name` | String   | 次龙头股票名称，显示次级涨停股票的名称。       |

### limit_daily_attribution - 涨停题材日内归因表
此表用于记录每只涨停股票当日归因的题材。

| 字段名           | 类型     | 描述                                             |
|----------------|----------|------------------------------------------------|
| `stock_code`   | String   | 股票代码，用于唯一标识股票，**主键**。                    |
| `trade_date`   | String   | 交易日期，用于标识数据属于哪一天，**主键**。                |
| `stock_name`   | String   | 股票名称，显示股票的具体名称。                                 |
| `concept_code` | String   | 与此股票关联的题材编码，用于查询股票涨停的题材。                     |
| `concept_name` | String   | 与此股票关联的题材名称，用于显示股票涨停的具体题材。                 |

### concepts_daily_data - 题材日线表
| 字段名          | 类型     | 描述                                     |
|---------------|----------|----------------------------------------|
| `concept_code`| String   | 题材编码，用于唯一标识一个题材，**主键**。          |
| `trade_date`  | String   | 交易日期，用于标识数据属于哪一天，**主键**。        |
| `open`        | Float    | 开盘价，显示题材当日的开盘价格。                       |
| `high`        | Float    | 最高价，显示题材当日的最高交易价格。                     |
| `low`         | Float    | 最低价，显示题材当日的最低交易价格。                     |
| `close`       | Float    | 收盘价，显示题材当日的收盘价格。                       |
| `pre_close`   | Float    | 昨收盘价，显示题材前一交易日的收盘价格。                 |
| `change`      | Float    | 变化额，显示题材当日的价格变化额。                      |
| `pct_chg`     | Float    | 百分比变化，显示题材当日的价格百分比变化。                |
| `vol`         | Float    | 成交量，显示题材当日的总成交量。                        |
| `amount`      | Float    | 成交金额，显示题材当日的总成交金额。                     |
| `turnover_rate` | Float  | 换手率，显示题材当日的换手率。                          |
| `avg_price`   | Float    | 平均价格，计算题材当日的平均交易价格。                   |

### trade_calendar - 交易日历表
此表用于记录交易所的开放和关闭状态。
| 字段名           | 类型     | 描述                                   |
|----------------|----------|--------------------------------------|
| `exchange`     | String   | 交易所标识，**主键**。                        |
| `cal_date`     | String   | 日历日期，**主键**。                          |
| `is_open`      | Integer  | 是否开市，1为开市，0为休市。                    |
| `pretrade_date`| String   | 上一个交易日日期。                          |

### stocks_daily_data - 股票日线表

此表记录每只股票每日的详细交易数据。

| 字段名        | 类型     | 描述                                  |
|-------------|----------|-------------------------------------|
| `ts_code`   | String   | 股票代码，**主键**。                         |
| `trade_date`| String   | 交易日期，**主键**。                         |
| `open`      | Float    | 开盘价。                                |
| `high`      | Float    | 最高价。                                |
| `low`       | Float    | 最低价。                                |
| `close`     | Float    | 收盘价。                                |
| `pre_close` | Float    | 昨收盘价。                              |
| `change`    | Float    | 价格变动额。                            |
| `pct_chg`   | Float    | 价格百分比变化。                         |
| `vol`       | Float    | 成交量。                                |
| `amount`    | Float    | 成交金额。                              |


### concept_info - 板块信息表

此表记录市场上的概念及其基本信息。
| 字段名               | 类型     | 描述                              |
|--------------------|----------|---------------------------------|
| `concept_code`     | String   | 概念编码，用于唯一标识一个概念，**主键**。   |
| `name`             | String   | 概念名称。                          |
| `count`            | Integer  | 包含的股票数量。                      |
| `exchange`         | String   | 交易所标识。                         |
| `list_date`        | String   | 概念的创建日期。                      |
| `type`             | String   | 概念类型。                           |
| `is_available`     | Integer  | 概念是否有效，1为有效，0为无效。           |
| `daily_data_start_date`| String| 概念数据起始日期。                  |
| `daily_data_end_date`  | String| 概念数据结束日期。                  |

### stock_base - 股票基本信息表

此表记录市场上的股票及其基本信息。
| 字段名          | 类型     | 描述                               |
|---------------|----------|----------------------------------|
| `ts_code`     | String   | 股票代码，**主键**。                      |
| `symbol`      | String   | 股票交易符号。                         |
| `name`        | String   | 股票名称。                            |
| `area`        | String   | 所在地区。                            |
| `industry`    | String   | 所属行业。                            |
| `market`      | String   | 市场类型（如主板、中小板、创业板等）。      |
| `exchange`    | String   | 交易所代码。                          |
| `list_status` | String   | 上市状态（L上市，D退市，P暂停上市）。       |
| `list_date`   | String   | 上市日期。                            |
| `delist_date` | String   | 退市日期。                            |
| `is_hs`       | String   | 是否为沪深港通标的，H是，S不是，N否。      |

### market_daily_data - 短线日内情绪指标数据表

此表记录短线市场每日的交易数据和情绪指数。
| 字段名                          | 类型     | 描述                                 |
|-------------------------------|----------|------------------------------------|
| `trade_date`                  | String   | 交易日期，**主键**。                       |
| `up_num`                      | Integer  | 上涨股票数量。                           |
| `down_num`                    | Integer  | 下跌股票数量。                           |
| `limit_up_num`                | Integer  | 涨停股票数量。                           |
| `non_one_word_limit_up_num`   | Integer  | 非一字板涨停股票数量。                    |
| `limit_down_num`              | Integer  | 跌停股票数量。                           |
| `prev_limit_up_high_open_rate`| Float    | 前一日涨停股次日高开率。                  |
| `prev_limit_up_success_rate`  | Float    | 前一日涨停股次日成功率。                  |
| `blow_up_rate`                | Float    | 炸板率。                                |
| `sentiment_index`             | Integer  | 情绪指数。                              |
| `physical_board_next_day_limit_up_rate`| Float| 物理板次日涨停率。                  |
| `limit_up_amount`             | Float    | 涨停股票的成交金额。                      |
| `highest_continuous_up_count` | Integer  | 最高连续上涨天数。                       |

### limit_up_tushare - 涨停股票数据表

此表记录通过TuShare获取的涨停股票数据。

| 字段名                     | 类型     | 描述                                 |
|--------------------------|----------|------------------------------------|
| `trade_date`             | String   | 交易日期，**主键**。                       |
| `stock_code`             | String   | 股票代码，**主键**。                       |
| `stock_name`             | String   | 股票名称。                              |
| `industry`               | String   | 所属行业。                              |
| `close`                  | Float    | 收盘价。                                |
| `pct_chg`                | Float    | 百分比变化。                             |
| `amount`                 | Float    | 成交金额。                              |
| `limit_amount`           | Float    | 涨停板成交金额。                         |
| `float_mv`               | Float    | 流通市值。                              |
| `total_mv`               | Float    | 总市值。                                |
| `turnover_ratio`         | Float    | 换手率。                                |
| `fd_amount`              | Float    | 融资额。                                |
| `first_time`             | String   | 首次触及涨停时间。                        |
| `last_time`              | String   | 最后触及涨停时间。                        |
| `open_times`             | Integer  | 打开次数（涨停板被打开的次数）。            |
| `up_stat`                | String   | 涨停统计。                              |
| `limit_times`            | Integer  | 涨停次数。                              |
| `limit_status`           | String   | 涨停状态。                              |
| `start_date`             | String   | 开始日期（用于统计周期内的数据）。           |
| `end_date`               | String   | 结束日期（用于统计周期内的数据）。           |
| `continuous_limit_up_count` | Integer  | 连续涨停次数。                          |
| `reason_type`            | String   | 涨停原因类型。                          |

### index_daily_data - 指数日线表

此表记录每个指数每日的详细交易数据。

| 字段名          | 类型     | 描述                               |
|---------------|----------|----------------------------------|
| `ts_code`     | String   | 指数代码，**主键**。                      |
| `trade_date`  | String   | 交易日期，**主键**。                      |
| `close`       | Float    | 收盘价。                            |
| `open`        | Float    | 开盘价。                            |
| `high`        | Float    | 最高价。                            |
| `low`         | Float    | 最低价。                            |
| `pre_close`   | Float   