from sqlalchemy import Column, String, Integer
from a_trade.db_base import Base

# 定义数据库模型
class WechatArticleInfo(Base):
    __tablename__ = 'wechat_article_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    link = Column(String, nullable=False, unique=True)
    create_time = Column(Integer, nullable=False)