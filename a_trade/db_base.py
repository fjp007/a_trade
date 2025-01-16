# coding: utf-8
# 文件：db_base.py

import os
from a_trade.settings import get_project_path
import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text, Float, TEXT
from sqlalchemy.orm import sessionmaker, declarative_base, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import logging
import traceback
from contextlib import contextmanager
import a_trade.settings

# 确保数据库目录存在
db_dir = get_project_path() / 'db'
db_dir.mkdir(parents=True, exist_ok=True)

# 数据库基础配置
Base = declarative_base()

# 主数据库配置
trade_db_path = db_dir / "a_data.db"
TRADE_DB_URL = f'sqlite:///{trade_db_path}'
engine = create_engine(TRADE_DB_URL, echo=False)
Session = scoped_session(sessionmaker(bind=engine))  # 主数据库全局持久化 Session
Base.metadata.create_all(engine)

# 备份数据库配置

def get_sync_engine():
    sync_db_path = db_dir / "sync.db"
    SYNC_DB_URL = f'sqlite:///{sync_db_path}'
    return create_engine(SYNC_DB_URL, echo=False)

def initialize_sync_db():
    engine = get_sync_engine()
    Base.metadata.create_all(engine)
    return engine

def initialize_sync_db():
    engine = get_sync_engine()
    Base.metadata.create_all(engine)
    return engine

def add_column_for_table(table_name, column_name, column_type, default_value=None):
    """为已有的表添加列，若列已存在则跳过"""
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    
    if column_name not in table.columns:
        column_type_str = column_type().compile(dialect=engine.dialect)
        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_str}"
        
        if default_value is not None:
            alter_sql += f" DEFAULT {default_value}"
        
        try:
            with engine.connect() as conn:
                conn.execute(text(alter_sql))
                print(f"Successfully added column '{column_name}' to '{table_name}'.")
        except SQLAlchemyError as e:
            print(f"Error adding column: {e}")
    else:
        print(f"Column '{column_name}' already exists in '{table_name}'.")

def get_recent_trade_date_in_table(table_name, field_name='trade_date', default_start_date='20200101'):
    """获取数据库表中的最近交易日期"""
    session = Session()
    try:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        query = session.query(table.c[field_name]).order_by(table.c[field_name].desc()).limit(1)
        last_trade_date = query.scalar()
        
        if last_trade_date:
            start_date = (datetime.datetime.strptime(last_trade_date, "%Y%m%d") + datetime.timedelta(days=1)).strftime("%Y%m%d")
        else:
            start_date = default_start_date
        return start_date
    finally:
        session.close()

def rename_table(old_table_name, new_table_name):
    """重命名数据库中的数据表"""
    alter_sql = text(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name}")
    try:
        with engine.connect() as conn:
            conn.execute(alter_sql)
            print(f"Successfully renamed table from '{old_table_name}' to '{new_table_name}'.")
    except SQLAlchemyError as e:
        print(f"Error renaming table: {e}")

def rename_column(table_name, old_column_name, new_column_name):
    """重命名指定表的列名为新列名"""
    alter_sql = text(f"ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name}")
    try:
        with engine.connect() as conn:
            conn.execute(alter_sql)
            print(f"Successfully renamed column from '{old_column_name}' to '{new_column_name}' in table '{table_name}'.")
    except SQLAlchemyError as e:
        print(f"Error renaming column: {e}")

def copy_table(source_engine, target_engine, table_name, primary_key_columns):
    """
    将指定表的数据从源数据库复制到目标数据库，使用UPSERT处理主键冲突。

    参数：
        source_engine (Engine): 源数据库的SQLAlchemy Engine对象。
        target_engine (Engine): 目标数据库的SQLAlchemy Engine对象。
        table_name (str): 要复制的数据表名称。
        primary_key_columns (list): 主键列的名称列表，用于处理唯一约束冲突。
    """
    source_session = sessionmaker(bind=source_engine)()
    target_session = sessionmaker(bind=target_engine)()
    
    try:
        # 获取源表结构
        source_metadata = MetaData()
        source_table = Table(table_name, source_metadata, autoload_with=source_engine)
        
        # 检查目标数据库是否存在该表
        target_metadata = MetaData()
        target_metadata.reflect(bind=target_engine)
        if table_name not in target_metadata.tables:
            # 创建表
            source_table.create(bind=target_engine)
            logging.info(f"在目标数据库中创建表 {table_name}")
            # 重新反射以获取新创建的表
            target_metadata.reflect(bind=target_engine)
        else:
            logging.info(f"表 {table_name} 已存在于目标数据库中，跳过创建")
        
        # 获取目标表
        target_table = target_metadata.tables[table_name]
        
        # 从源表读取所有数据
        data = source_session.query(source_table).all()
        
        if data:
            # 将每一行数据转换为字典
            rows = [
                {column.name: getattr(row, column.name) for column in source_table.columns}
                for row in data
            ]
            
            # 使用事务进行批量插入或更新
            with target_engine.begin() as conn:
                for row in rows:
                    stmt = sqlite_insert(target_table).values(**row)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=primary_key_columns,  # 主键列
                        set_=row
                    )
                    conn.execute(stmt)
            logging.info(f"成功将表 {table_name} 的数据复制到目标数据库，共复制 {len(data)} 条记录")
            
            # 验证插入的数据
            with target_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                logging.info(f"目标数据库中表 {table_name} 当前记录数: {count}")
        else:
            logging.info(f"表 {table_name} 为空，无需复制数据")
        
    except Exception as e:
        logging.error(f"复制表 {table_name} 数据到目标数据库失败: {e}")
        logging.error(traceback.format_exc())
    finally:
        source_session.close()
        target_session.close()

def merge_db_data_from_base_to_sync():
    tables_to_sync = {
        'limit_reason_to_concept': ['limit_reason'],
    }
    sync_engine = initialize_sync_db()
    for table_name, primary_key_columns in tables_to_sync.items():
        copy_table(
            source_engine=engine,
            target_engine=sync_engine,
            table_name=table_name,
            primary_key_columns=primary_key_columns
        )
    logging.info('数据已同步至云端增量数据库')

def merge_db_data_from_sync_to_base():
    tables_to_sync = {
        'limit_reason_to_concept': ['limit_reason'],
    }
    sync_engine = initialize_sync_db()
    for table_name, primary_key_columns in tables_to_sync.items():
        copy_table(
            source_engine=sync_engine,
            target_engine=engine,
            table_name=table_name,
            primary_key_columns=primary_key_columns
        )
    logging.info('数据已同步至云端主数据库')

if __name__ == "__main__":
    # 使用示例：增加新列
    # add_column_for_table('wechat_limit_article', 'image_url', TEXT)
    # rename_column('limit_reason_to_concept', 'TEXconcept_namesT', 'concept_names')

    merge_db_data_from_base_to_sync()
