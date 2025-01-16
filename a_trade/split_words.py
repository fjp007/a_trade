# coding: utf-8
import jieba
from a_trade.db_base import Session
import logging
from a_trade.settings import get_project_path

def get_frequecy_dict_path():
    project_path = get_project_path()
    frequency_dict_path = project_path / 'concept_slip_words_frequecy.txt'
    return frequency_dict_path

def load_custom_dictionary():
    """加载自定义词典"""
    frequency_dict_path = get_frequecy_dict_path()
    
    # 检查文件是否存在
    if not frequency_dict_path.is_file():
        logging.error(f"板块分词频率数据文件未找到: {frequency_dict_path}")
        raise FileNotFoundError(f"板块分词频率数据文件未找到: {frequency_dict_path}")
    
    # 将 Path 对象转换为字符串路径
    try:
        jieba.load_userdict(str(frequency_dict_path))
        logging.info(f"已加载板块分词频率数据: {frequency_dict_path}")
    except Exception as e:
        logging.error(f"板块分词频率数据时出错: {e}")
        raise

load_custom_dictionary()

def prepare_split_concepts():
    from a_trade.concept_manager import conceptManager

    session = Session()
    concept_word_to_frequecy = {}
    result = conceptManager.concept_info_cache.values()
    ignore_words = ['(',')','Ⅲ']
    # 统计词频
    for concept_info in result:
        concept_name = concept_info.name
        words = jieba.lcut(concept_name)
        for word in words:
            if word not in ignore_words:
                if word in concept_word_to_frequecy:
                    concept_word_to_frequecy[word] += 1
                else:
                    concept_word_to_frequecy[word] = 1

    # 将词频按降序排序
    sorted_concept_word_to_frequecy = dict(sorted(concept_word_to_frequecy.items(), key=lambda item: item[1], reverse=True))
    frequency_dict_path = get_frequecy_dict_path()
    # 将词频放大1000倍并写入user_dict.txt文件
    with open(frequency_dict_path, 'w', encoding='utf-8') as f:
        for word, freq in sorted_concept_word_to_frequecy.items():
            adjusted_freq = freq * 100000000
            f.write(f"{word} {adjusted_freq}\n")

    print(f"概念板块词频因子统计成功: {frequency_dict_path}")

if __name__ == "__main__":
    prepare_split_concepts()