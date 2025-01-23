# coding: utf-8
import jieba
import logging
from a_trade.settings import get_project_path
from pathlib import Path
from typing import Dict, List, NoReturn, Set, Any
from a_trade.concept_manager import ConceptManager

def get_frequecy_dict_path() -> Path:
    """获取词频字典路径
    
    Returns:
        Path: 词频字典文件路径
    """
    project_path = get_project_path()
    frequency_dict_path = project_path / 'concept_slip_words_frequecy.txt'
    return frequency_dict_path

def load_custom_dictionary() -> NoReturn:
    """加载自定义词典
    
    Raises:
        FileNotFoundError: 当词频字典文件不存在时
        Exception: 加载字典时发生错误
    """
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

def prepare_split_concepts() -> NoReturn:
    """准备分词概念数据
    
    统计概念词频并写入词频字典文件
    """
    concept_word_to_frequecy: Dict[str, int] = {}
    result = ConceptManager.concept_info_cache.values()
    ignore_words: Set[str] = {'(', ')', 'Ⅲ'}
    
    # 统计词频
    for concept_info in result:
        concept_name: str = concept_info.name
        words: List[str] = jieba.lcut(concept_name)
        for word in words:
            if word not in ignore_words:
                if word in concept_word_to_frequecy:
                    concept_word_to_frequecy[word] += 1
                else:
                    concept_word_to_frequecy[word] = 1

    # 将词频按降序排序
    sorted_concept_word_to_frequecy: Dict[str, int] = dict(
        sorted(concept_word_to_frequecy.items(), key=lambda item: item[1], reverse=True)
    )
    frequency_dict_path: Path = get_frequecy_dict_path()
    
    # 将词频放大1000倍并写入user_dict.txt文件
    with open(frequency_dict_path, 'w', encoding='utf-8') as f:
        for word, freq in sorted_concept_word_to_frequecy.items():
            adjusted_freq: int = freq * 100000000
            f.write(f"{word} {adjusted_freq}\n")

    print(f"概念板块词频因子统计成功: {frequency_dict_path}")

if __name__ == "__main__":
    prepare_split_concepts()