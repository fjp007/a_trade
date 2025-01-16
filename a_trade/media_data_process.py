# coding: utf-8

import re
import logging
import os
import shutil
from sqlalchemy import Column, String, Integer, TEXT
from sqlalchemy.exc import SQLAlchemyError

import traceback
import cv2
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from a_trade.media_data_spider import WechatArticleInfo
from a_trade.settings import get_project_path
from a_trade.db_base import Session, Base, engine
from a_trade.trade_calendar import TradeCalendar

# 定义新的数据库模型
class WechatLimitArticle(Base):
    __tablename__ = 'wechat_limit_article'
    trade_date = Column(String, primary_key=True)  # YYYYMMDD
    title = Column(String, nullable=False)
    link = Column(String, nullable=False, unique=True)
    create_time = Column(Integer, nullable=False)
    image_url = Column(TEXT)

Base.metadata.create_all(engine)

def clean_title(title: str) -> str:
    """
    移除标题前后的所有空白字符，包括常规空格、全角空格、零宽空格等。
    """
    # 使用 Unicode 的空白字符类别来匹配所有空白字符
    # \s 在 Unicode 模式下匹配所有常规空白字符，但不包括零宽空格 \u200b
    # 因此，手动添加 \u200b 进行匹配
    cleaned_title = re.sub(r'^[\s\u200b]+|[\s\u200b]+$', '', title, flags=re.UNICODE)
    logging.debug(f"清洗前的标题: {repr(title)}")
    logging.debug(f"清洗后的标题: {repr(cleaned_title)}")
    return cleaned_title

def matches_title_format(title: str) -> bool:
    """
    判断标题是否符合指定格式：
    格式1: X月X日 涨停板复盘
    格式2: X月X日复盘数据
    """
    title = clean_title(title)
    pattern1 = r'^\d{1,2}月\d{1,2}日\s*涨停板复盘\s*$'
    pattern2 = r'^\d{1,2}月\d{1,2}日\s*复盘数据\s*$'
    return bool(re.match(pattern1, title)) or bool(re.match(pattern2, title))

def extract_month_day(title: str):
    """
    从标题中提取月份和日期。
    返回 (month, day) 的元组，如果提取失败则返回 (None, None)。
    """
    title = clean_title(title)
    pattern = r'^(\d{1,2})月(\d{1,2})日'
    match = re.match(pattern, title)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        return month, day
    return None, None

def process_articles(session):
    """处理并筛选符合条件的文章，写入新表。"""

    # 清空 WechatLimitArticle 表中的所有数据
    try:
        deleted = session.query(WechatLimitArticle).delete()
        session.commit()
        logging.info(f"已清空 'wechat_limit_article' 表中的 {deleted} 条记录。")
    except Exception as e:
        session.rollback()
        logging.error(f"清空 'wechat_limit_article' 表时发生错误: {e}")
        return

    # 查询所有文章，按创建时间降序排序
    try:
        articles = session.query(WechatArticleInfo).order_by(WechatArticleInfo.create_time.desc()).all()
    except Exception as e:
        logging.error(f"查询 'wechat_article_info' 表时发生错误: {e}")
        return

    logging.info(f"总共找到 {len(articles)} 篇文章需要处理。")

    # 定义要插入的新文章字典，键为 trade_date
    new_filtered_articles = {}
    added_trade_dates = set()  # 用于跟踪本次运行中已添加的 trade_date
    added_links = set()         # 用于跟踪本次运行中已添加的 link

    # 初始化年份和上一篇文章的月份
    current_year = 2025
    prev_month = None

    for article in articles:
        logging.debug(f"处理数据 {repr(article.title)}")
        title = clean_title(article.title)
        if matches_title_format(title):
            logging.debug(f"匹配命中 {repr(article.title)}")
            month, day = extract_month_day(title)
            if month is None or day is None:
                logging.warning(f"无法从标题中提取月份和日期: {repr(title)}")
                continue
            logging.debug(f"提取到的月份和日期: {month}月{day}日")

            # 判断是否需要递减年份
            if prev_month is not None and month > prev_month:
                current_year -= 1
                logging.info(f"月份从 {prev_month} 月降至 {month} 月，年份减1，现在年份为 {current_year} 年。")

            trade_date = f"{current_year}{month:02d}{day:02d}"
            logging.debug(f"生成的 trade_date: {trade_date}")

            # 检查 trade_date 是否已在本次插入中存在，避免主键冲突
            if trade_date in added_trade_dates:
                logging.warning(f"trade_date {trade_date} 已在本次插入中存在，跳过插入。")
                # 打印重复数据
                duplicate_article = WechatLimitArticle(
                    trade_date=trade_date,
                    title=title,
                    link=article.link,
                    create_time=article.create_time
                )
                logging.warning(f"重复插入的数据: trade_date={duplicate_article.trade_date}, title={duplicate_article.title}, link={duplicate_article.link}, create_time={duplicate_article.create_time}")
                continue

            # 检查 link 是否已在本次插入中存在，避免唯一约束冲突
            if article.link in added_links:
                logging.warning(f"link {article.link} 已在本次插入中存在，跳过插入。")
                # 打印重复数据
                duplicate_article = WechatLimitArticle(
                    trade_date=trade_date,
                    title=title,
                    link=article.link,
                    create_time=article.create_time
                )
                logging.warning(f"重复插入的数据: trade_date={duplicate_article.trade_date}, title={duplicate_article.title}, link={duplicate_article.link}, create_time={duplicate_article.create_time}")
                continue

            # 构建新文章对象
            try:
                filtered_article = WechatLimitArticle(
                    trade_date=trade_date,
                    title=title,
                    link=article.link,
                    create_time=article.create_time
                )
            except Exception as e:
                logging.error(f"构建 WechatLimitArticle 对象时发生错误: {e}")
                continue

            # 添加到字典
            new_filtered_articles[trade_date] = filtered_article
            added_trade_dates.add(trade_date)
            added_links.add(article.link)
            logging.info(f"添加到新表的数据 - trade_date: {trade_date}, 标题: {title}, 链接: {article.link}, 创建时间: {article.create_time}")

            # 更新上一篇文章的月份
            prev_month = month
        else:
            # 记录未匹配的标题
            logging.warning(f"标题不匹配: {repr(title)}")

    # 批量插入新文章
    if new_filtered_articles:
        try:
            session.bulk_save_objects(new_filtered_articles.values())
            session.commit()
            logging.info(f"成功批量插入 {len(new_filtered_articles)} 篇符合条件的文章到 'wechat_limit_article' 表。")
        except Exception as e:
            session.rollback()
            logging.error(f"批量插入新表时发生错误: {e}")
            logging.error(traceback.format_exc())
    else:
        logging.info("没有符合条件的文章需要插入。")

def main():
    # 创建数据库会话
    session = Session()
    
    try:
        process_articles(session)
    except Exception as e:
        logging.error(f"处理文章时发生未预期的错误: {e}")
        logging.error(traceback.format_exc())
    finally:
        session.close()

def valid_wechat_data(trade_date):
    with Session() as session:
        result = session.query(WechatLimitArticle).filter(WechatLimitArticle.trade_date == trade_date).first()
        if not result:
            print(f'{trade_date} has no article')

def fetch_and_update_image_url(start_date, end_date):
    """
    从 WechatLimitArticle 获取指定日期范围内的文章链接，通过 Selenium 打开链接，提取 PNG 地址，
    并更新到数据库的 image_url 字段。

    参数:
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
    """
    # 创建数据库会话
    session = Session()
    
    # 查询指定日期范围内的文章
    articles = session.query(WechatLimitArticle).filter(
        WechatLimitArticle.trade_date >= start_date,
        WechatLimitArticle.trade_date <= end_date
    ).all()

    if not articles:
        logging.warning(f"未找到日期范围 {start_date} 到 {end_date} 的文章。")
        return


    # 配置 Selenium WebDriver
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    service = Service(os.getenv('CHROME_DRIVER_PATH'))  # 修改为实际的 chromedriver 路径
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        for article in articles:
            logging.info(f"正在处理 trade_date 为 {article.trade_date} 的文章，链接: {article.link}")

            try:
                # 打开文章链接
                driver.get(article.link)

                # 等待图片的出现
                try:
                    image_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '#js_content img.rich_pages.wxw-img'))
                    )
                    image_url = image_element.get_attribute('data-src')
                except Exception as e:
                    logging.error(f"未能找到图片元素: {e}")
                image_element = driver.find_element(By.CSS_SELECTOR, '#js_content img.rich_pages.wxw-img')
                image_url = image_element.get_attribute('data-src')

                if not image_url:
                    logging.warning(f"未找到 trade_date 为 {article.trade_date} 的文章的图片地址。")
                    continue

                logging.info(f"提取到的图片地址: {image_url}")

                # 更新数据库中的 image_url 字段
                article.image_url = image_url
                session.commit()
                logging.info(f"成功更新 trade_date 为 {article.trade_date} 的文章的 image_url 字段。")
            except SQLAlchemyError as e:
                session.rollback()
                logging.error(f"数据库更新失败: {e}")
            except Exception as e:
                logging.error(f"处理 trade_date 为 {article.trade_date} 的文章时发生错误: {e}")

    finally:
        session.close()
        driver.quit()

def fetch_and_update_image_url(start_date, end_date):
    """
    从 WechatLimitArticle 获取指定日期范围内的文章链接，通过 Selenium 打开链接，提取 PNG 地址，
    并更新到数据库的 image_url 字段。

    参数:
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
    """
    # 创建数据库会话
    session = Session()
    
    # 查询指定日期范围内的文章
    articles = session.query(WechatLimitArticle).filter(
        WechatLimitArticle.trade_date >= start_date,
        WechatLimitArticle.trade_date <= end_date
    ).all()

    if not articles:
        logging.warning(f"未找到日期范围 {start_date} 到 {end_date} 的文章。")
        return

    logging.info(f"总共找到 {len(articles)} 篇文章需要处理。")

    # 配置 Selenium WebDriver
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    service = Service(os.getenv('CHROME_DRIVER_PATH'))  # 修改为实际的 chromedriver 路径
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        for article in articles:
            logging.info(f"正在处理 trade_date 为 {article.trade_date} 的文章，链接: {article.link}")

            try:
                # 打开文章链接
                driver.get(article.link)

                # 查找图片元素
                image_element = driver.find_element(By.CSS_SELECTOR, '#js_content img.rich_pages.wxw-img')
                image_url = image_element.get_attribute('data-src')

                if not image_url:
                    logging.warning(f"未找到 trade_date 为 {article.trade_date} 的文章的图片地址。")
                    continue

                logging.info(f"提取到的图片地址: {image_url}")

                # 更新数据库中的 image_url 字段
                article.image_url = image_url
                session.commit()
                logging.info(f"成功更新 trade_date 为 {article.trade_date} 的文章的 image_url 字段。")
            except SQLAlchemyError as e:
                session.rollback()
                logging.error(f"数据库更新失败: {e}")
            except Exception as e:
                logging.error(f"处理 trade_date 为 {article.trade_date} 的文章时发生错误: {e}")

    finally:
        session.close()
        driver.quit()

def download_images_by_date_range(start_date, end_date):
    """
    根据 WechatLimitArticle 中的 trade_date 和 image_url 下载图片，
    保存到项目根目录的 media_data/{trade_date}/daily_review.png。

    参数:
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
    """
    import requests

    # 创建数据库会话
    session = Session()

    try:
        # 查询指定日期范围内的文章
        articles = session.query(WechatLimitArticle).filter(
            WechatLimitArticle.trade_date >= start_date,
            WechatLimitArticle.trade_date <= end_date,
            WechatLimitArticle.image_url != None
        ).all()

        if not articles:
            logging.warning(f"未找到日期范围 {start_date} 到 {end_date} 的文章或图片地址。")
            return

        for article in articles:
            trade_date = article.trade_date
            image_url = article.image_url

            # 创建存储路径
            directory = os.path.join(get_project_path(), 'media_data', trade_date)
            os.makedirs(directory, exist_ok=True)
            image_path = os.path.join(directory, 'daily_review.png')

            try:
                # 下载图片
                response = requests.get(image_url, stream=True)
                if response.status_code == 200:
                    with open(image_path, 'wb') as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
                    logging.info(f"图片已成功保存到 {image_path}")
                else:
                    logging.warning(f"无法下载图片，HTTP 状态码: {response.status_code}, URL: {image_url}")
            except Exception as e:
                logging.error(f"下载图片失败: {e}, URL: {image_url}")

    finally:
        session.close()

def split_tables_by_blank_rows(
    input_image_path, 
    output_dir=None,
    row_empty_ratio=0.98,    # 单行白色像素比例大于该值认为是空行
    min_table_height=90,     # 如果切出的表格块高度小于这个值就丢弃
    min_blank_height=25       # 如果空白区块行数小于该值，就不做分隔
):
    """
    基于行扫描、寻找空白行，来切分多张上下排列的表格，并额外过滤“过小空白块”。
    
    :param input_image_path:   输入图片路径
    :param output_dir:         若指定，将裁剪后的子图保存到此文件夹
    :param row_empty_ratio:    当某一行“白色”像素比例超过该值，视为空行
    :param min_table_height:   切出来的表格若高度<此值，丢弃
    :param min_blank_height:   空白区块若行数<此值，视为噪声，不做有效分隔
    :return:                   [(y1, y2, sub_img), ...]，包含每个子表格的上下坐标与子图
    """
    def is_white_or_watermark_pixel(pixel, white_threshold=200):
        """
        判断像素是否是“白色”
        """
        return pixel >= white_threshold
    
    # 1. 读取图像
    img = cv2.imread(input_image_path)
    if img is None:
        raise FileNotFoundError(f"无法读取图片: {input_image_path}")
    
    # 2. 获取图像尺寸
    height, width, _ = img.shape

    # 3. 转灰度
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # 4. 扫描每一行的白色像素比例，判断是否空行
    empty_lines = []
    for row_idx in range(height):
        row_pixels = gray[row_idx, :]  
        white_pixel_count = np.sum(
            [is_white_or_watermark_pixel(pixel) for pixel in row_pixels]
        )
        white_ratio = white_pixel_count / float(width)
        
        if white_ratio >= row_empty_ratio:
            empty_lines.append(row_idx)

    # 如果没有空行，则无法按“空白区块”来分割
    if len(empty_lines) == 0:
        print("未检测到明显空行，可能只有一张大表格或背景干扰大。")
        return []

    # 5. 将连续的空行合并成“空白区块”[(start_line, end_line), ...]
    line_groups = []
    group_start = empty_lines[0]
    prev_line = empty_lines[0]

    for i in range(1, len(empty_lines)):
        this_line = empty_lines[i]
        # 若与前一行不连续(行差>1)，说明分段
        if this_line - prev_line > 1:
            line_groups.append((group_start, prev_line))
            group_start = this_line
        prev_line = this_line
    # 最后一段
    line_groups.append((group_start, prev_line))

    # 6. 过滤那些高度太小的空白区块
    #    只有当 (end - start + 1) >= min_blank_height 时，才算有效的表格分隔
    valid_groups = []
    for (g_start, g_end) in line_groups:
        blank_size = g_end - g_start + 1
        if blank_size >= min_blank_height:
            valid_groups.append((g_start, g_end))

    # 若过滤后为空，则说明并没有足够大的空白区块可以分隔表格
    if len(valid_groups) == 0:
        print("空白区块都太小，无法使用它们来分隔表格；可能仍是一整张表格。")
        return []

    # 7. 根据这些“有效的空白区块”，来切分多个表格区域
    sub_tables = []

    # (7.1) 处理图像顶部 -> 第1个空白区块之间
    if valid_groups[0][0] > 0:
        top_y = 0
        bottom_y = valid_groups[0][0]
        if bottom_y - top_y >= min_table_height:
            sub_tables.append((top_y, bottom_y, img[top_y:bottom_y, 0:width]))

    # (7.2) 相邻空白区块之间
    for i in range(len(valid_groups) - 1):
        _, prev_bottom = valid_groups[i]
        next_top, _ = valid_groups[i + 1]

        top_y = prev_bottom
        bottom_y = next_top

        if bottom_y - top_y >= min_table_height:
            sub_tables.append((top_y, bottom_y, img[top_y:bottom_y, 0:width]))

    # (7.3) 最后一段空白区块 -> 图像底部
    if valid_groups[-1][1] < height:
        top_y = valid_groups[-1][1]
        bottom_y = height
        if bottom_y - top_y >= min_table_height:
            sub_tables.append((top_y, bottom_y, img[top_y:bottom_y, 0:width]))

    # 8. 若需要保存结果
    if output_dir and len(sub_tables) > 0:
        os.makedirs(output_dir, exist_ok=True)
        for idx, (start_y, end_y, table_img) in enumerate(sub_tables):
            save_path = os.path.join(output_dir, f"table_{idx}.png")
            cv2.imwrite(save_path, table_img)
            print(f"{input_image_path}裁切 表格区域 {idx} 已保存: {save_path}")

    return sub_tables

def process_images_in_date_range(start_date, end_date):
    """
    对指定日期范围内下载的 daily_review.png 执行表格裁切，裁切结果保存到对应目录下。

    参数:
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
    """
    def delete_table_png(trade_date):
        # 构造目标目录路径
        output_dir = os.path.join(get_project_path(), 'media_data', trade_date, 'tables')

        # 检查目录是否存在
        if os.path.exists(output_dir):
            # 删除目录及其所有内容
            shutil.rmtree(output_dir)
            print(f"删除 {trade_date} 裁切子图")
        else:
            print(f"未找到{trade_date} 裁切子图目录")
        
    TradeCalendar.iterate_trade_days(start_date, end_date, delete_table_png)

    # 创建数据库会话
    session = Session()

    try:
        # 查询指定日期范围内的文章
        articles = session.query(WechatLimitArticle).filter(
            WechatLimitArticle.trade_date >= start_date,
            WechatLimitArticle.trade_date <= end_date
        ).all()

        if not articles:
            logging.warning(f"未找到日期范围 {start_date} 到 {end_date} 的文章。")
            return

        logging.info(f"总共找到 {len(articles)} 篇文章需要处理表格裁切。")

        for article in articles:
            trade_date = article.trade_date
            image_path = os.path.join(get_project_path(), 'media_data', trade_date, 'daily_review.png')
            output_dir = os.path.join(get_project_path(), 'media_data', trade_date, 'tables')

            if not os.path.exists(image_path):
                logging.warning(f"图片文件不存在: {image_path}")
                continue

            logging.info(f"正在处理图片裁切: {image_path}")

            try:
                split_tables_by_blank_rows(
                    input_image_path=image_path,
                    output_dir=output_dir
                )
            except Exception as e:
                logging.error(f"图片裁切失败: {e}, 文件: {image_path}")

    finally:
        session.close()

def is_black_background(input_image_path):
    def is_black_pixel(pixel, black_threshold=20):
        """
        判断像素是否是黑色”）。
        """
        return pixel <= black_threshold
    
    # 1. 读取图像
    img = cv2.imread(input_image_path)
    if img is None:
        raise FileNotFoundError(f"无法读取图片: {input_image_path}")
    
    # 2. 获取图像尺寸
    height, width, _ = img.shape

    # 3. 转灰度
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # 4. 扫描每一行的白色像素比例，判断是否空行
    black_lines = []
    for row_idx in range(height):
        row_pixels = gray[row_idx, :]
        black_pixel_count = np.sum(
            [is_black_pixel(pixel) for pixel in row_pixels]
        )
        black_ratio = black_pixel_count / float(width)
        if black_ratio > 0.8:
            black_lines.append(row_idx)
    return len(black_lines) > 200


# 示例调用
if __name__ == "__main__":
    import sys
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    process_images_in_date_range(start_date, end_date)
    # test('/Users/fengjianpeng/Documents/Leon/Workspace/a_trade/media_data/20220620/tables/table_2.png')