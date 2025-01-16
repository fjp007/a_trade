# coding: utf-8

import time
import logging
from typing import List

from wechatarticles import PublicAccountsWeb
from sqlalchemy import Column, String, Integer
from a_trade.db_base import Session as DBSession, Base, engine

# 定义数据库模型
class WechatArticleInfo(Base):
    __tablename__ = 'wechat_article_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    link = Column(String, nullable=False, unique=True)
    create_time = Column(Integer, nullable=False)

def clean_title(title: str) -> str:
    """移除标题前后的空格。"""
    return title.strip()

def insert_articles(session, articles: List[dict], page: int):
    """将文章列表批量插入数据库。"""
    try:
        # 获取所有要插入的链接
        links = [article.get('link', '') for article in articles]
        
        # 查询数据库中已存在的链接，避免重复插入
        existing_links = set(
            link for (link,) in session.query(WechatArticleInfo.link).filter(WechatArticleInfo.link.in_(links)).all()
        )
        
        # 过滤掉已存在的文章
        new_articles = []
        for article in articles:
            link = article.get('link', '')
            title = article.get('title', '')
            if link not in existing_links:
                title = clean_title(title)
                create_time = int(article.get('create_time', 0))
                print(f"[标题] {title}")
                print(f"[链接] {link}")
                print(f"[创建时间] {create_time}")
                
                new_articles.append({
                    'title': title,
                    'link': link,
                    'create_time': create_time
                })
            else:
                logging.info(f"文章已存在 - 标题: {title} 链接: {link}")
        
        if new_articles:
            # 使用 bulk_insert_mappings 进行批量插入
            session.bulk_insert_mappings(WechatArticleInfo, new_articles)
            session.commit()
            logging.info(f"第 {page} - {page+4} 页：成功批量插入 {len(new_articles)} 篇文章")
        else:
            logging.info(f"第 {page} - {page+4} 页：没有新文章需要插入")
    except Exception as e:
        session.rollback()
        logging.error(f"批量插入文章失败。错误: {e}")

def fetch_and_store_articles(paw: PublicAccountsWeb, nickname: str, biz: str, count: int = 5, delay: int = 180):
    """循环抓取文章并存储到数据库。"""
    session = DBSession()
    page = 12  # 页数从0开始

    while True:
        try:
            logging.info(f"正在从第 {page} 天开始爬取，范围包含 {count} 天数据")
            article_data = paw.get_urls(nickname, biz=biz, begin=str(page), count=str(count))
            
            if not article_data:
                logging.info("未发现更多文章，退出循环。")
                break

            insert_articles(session, article_data, page)
            
            # 准备下一次抓取
            page += count

            logging.info(f"等待 {delay} 秒后继续抓取下一次...")
            time.sleep(delay)
        except Exception as e:
            logging.error(f"抓取或存储文章时发生错误: {e}")
            logging.info(f"等待 {delay} 秒后重试...")
            time.sleep(delay)
    session.close()

if __name__ == "__main__":
    # 配置
    cookie = "pgv_pvid=1676118173007754; rewardsn=; wxtokenkey=777; wwapp.vid=; wwapp.cst=; wwapp.deviceid=; pgv_info=ssid=s2093069523; pvpqqcomrouteLine=newsDetail; tokenParams=%3Ftid%3D599495; fqm_sessionid=4a12f3c6-895c-4b1a-a397-41b30a713a3a; _qimei_q36=; _qimei_h38=e5b4e7a8503779aea31526b903000009b17b10; pac_uid=0_tbQRt0e9GWfpD; _qimei_fingerprint=97fa8c59b463476119937acf40c14b68; suid=user_0_tbQRt0e9GWfpD; qq_domain_video_guid_verify=a4cff49472db2a46; vversion_name=8.2.95; video_omgid=a4cff49472db2a46; poc_sid=HNk9gmejqRZhK0jp1m0nq0ACAK4Z0vxgKq8dla2O; mm_lang=zh_CN; ua_id=OvBwfiQJf0ECQCutAAAAADVfz2pY0m-BMVciDDHwgD0=; wxuin=36609710046000; _clck=10qmh7w|1|fsh|0; mm_lang=zh_CN; cert=Dz707GXoXeN182UdcnSEgkO1tkuKAm86; uuid=9ba9bf7f7333448117aacaa40cd19a56; rand_info=CAESIPaW8IwiHaOeWImxF61F+eIGEgEd0Zsvv9Efjl9531Op; slave_bizuin=3598836919; data_bizuin=3598836919; bizuin=3598836919; data_ticket=VFVpBykft6uXpWCBKwZPX4L0bVX9fJlubHg5J9ZbfAy0JsinhppOusZZdySsgDxs; slave_sid=dVlZUjF0YkR0NzFWQkFZWHM5bmZCWjJXSmkyTkhhMVM5cngyZnl3ZVpZZTN2NjE1TEhBTVNIUkF6TmI5dFQwczRsWU9EWUU3VlhmaEh5c0xBY1huUXU4Ujh4cVVWSXJZUjRQVGw2S0ZZSkNPQ3lLY2tOZWszbTd0NWR2WHB6Ykc3Z3h6ZHRNQ1VnSmNWdENp; slave_user=gh_75e2a2c0cdcb; xid=b45ecc371f497f879b2aa7a2871e0d04; _clsk=1izobk1|1736609971046|5|1|mp.weixin.qq.com/weheat-agent/payload/record"
    token = "856244490"
    nickname = "看懂龙头股"
    biz = "MzAxMDYxNDQ4NA=="

    # 初始化 PublicAccountsWeb
    paw = PublicAccountsWeb(cookie=cookie, token=token)

    # 开始抓取并存储文章
    fetch_and_store_articles(paw, nickname, biz)