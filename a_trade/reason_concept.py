# coding: utf-8
from a_trade.db_base import Base, Session
from sqlalchemy import Column, String, or_, and_
import os
import json
from a_trade.trade_utils import strip_list, concepts_equal
import logging
import jieba
from a_trade.baidu_translate import translate_to_chinese
from a_trade.concept_llm_analysis import analyze_related_concept_with_limit_reason, analyze_key_word_with_limit_reason
from a_trade.bing_caller import search_word_from_bing
import re
from a_trade.settings import get_project_path
import a_trade.split_words
import random
import time
from a_trade.concept_manager import ConceptInfo, target_conditions, conceptManager
from a_trade.concept_relations import conceptRelations

JSON_FILE_PATH = get_project_path() / "custom_concept_words.json"

class LimitReasonToConcept(Base):
     # 涨停原因映射板块名称表
    __tablename__ = 'limit_reason_to_concept'
    limit_reason = Column(String, primary_key=True)
    pre_concept_names = Column(String)
    concept_names = Column(String)

class ReasonConceptManger:
    def __init__(self):
        self.custom_concept_words = self._load_custom_concepts()
        # 用于缓存的字典
        self.reason_to_concept_cache = {}

        self.return_words = ['天板','跌停','涨停','分红','小盘股','降价','异常','波动','减持','一字加速', '妖股','成交量','其它', 'H股']

        # 885338 - 融资融券
        # 885694 - 深股通
        # 885520 - 沪股通
        # 885867- 标普道琼斯A股
        # 883304 - 中证500成份股
        # 883303 - 上证380成份股
        # 883300 - 沪深300样本股
        # 885916 - 同花顺漂亮100
        self.ignore_concepts = [
            "融资融券", "深股通", "沪股通", "标普道琼斯A股", "中证500成份股", "上证380成份股", "沪深300样本股", "同花顺漂亮100"
        ]

    def _load_custom_concepts(self):
        """
        读取 JSON 文件中的板块关键词数据
        """
        
        if not os.path.exists(JSON_FILE_PATH):
            with open(JSON_FILE_PATH, 'w', encoding='utf-8') as file:
                json.dump({}, file, ensure_ascii=False, indent=4)
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as file:
            return json.load(file)
        
    def add_concept_keywords(self, concept_name, keywords):
        """
        新增板块或为已有板块新增关键词
        """
        # 确保 keywords 是列表格式
        if isinstance(keywords, str):
            keywords = [keywords]
        
        if concept_name in self.custom_concept_words:
            # 合并去重
            self.custom_concept_words[concept_name] = list(set(self.custom_concept_words[concept_name] + keywords))
        else:
            # 新增板块
            self.custom_concept_words[concept_name] = keywords

        with open(JSON_FILE_PATH, 'w', encoding='utf-8') as file:
            json.dump(self.custom_concept_words, file, ensure_ascii=False, indent=4)

    def _get_concept_from_reason(self, reason_str):
        # 检查缓存中是否已经有这个原因的板块数据
        if reason_str in self.reason_to_concept_cache:
            return self.reason_to_concept_cache[reason_str]

        session = Session()
        try:
            # 在数据表limit_reason_to_concept中查询limit_reason
            existing_mapping = session.query(LimitReasonToConcept).filter(LimitReasonToConcept.limit_reason == reason_str).first()

            if existing_mapping:
                if existing_mapping.concept_names:
                    concepts = existing_mapping.concept_names.split(',')
                    self.reason_to_concept_cache[reason_str] = concepts  # 缓存结果
                    return concepts
                else:
                    self.reason_to_concept_cache[reason_str] = None  # 缓存空结果
                    return None
            else:
                logging.info(f"{reason_str} 未记录在数据表里")
                concept_names = self.analyze_concept_from_reason(reason_str)
                self.reason_to_concept_cache[reason_str] = concept_names  # 缓存新分析的结果
                return concept_names
        finally:
            session.close()

    def _is_pure_punctuation(self, word):
        """
        判断字符串是否为纯标点符号（包括中文和英文标点符号）。
        """
        punctuation_chars = (
            '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'  # 英文标点符号
            '，。、；：？！…（）【】《》‘’“”'  # 中文标点符号
        )
        return all(char in punctuation_chars for char in word)

    def _is_valid_word(self, word):
        # 没有意义的分词可以忽略，不加入板块匹配条件
        ignore_words = ['(',')','概念','Ⅲ','其他', '100','%','及','产品', '或','股','和', '新', '人','拟','A股','市值','股票','小盘','服务', 'Ⅳ', 'Ⅱ','大', '材料', '装备', '设备', '高端', '制品']
        if not word:  # 跳过空字符串和单字符
            return False
        if word in ignore_words:  # 跳过无意义分词
            return False
        if self._is_pure_punctuation(word):  # 跳过纯标点符号
            return False
        return True

    def _query_concepts_from_split_words(self, reason_str, words):
        final_words = []
        related_custom_concepts = set()  # 用于存储关联的自定义板块
        for word in words:
            word = word.strip()
            if not self._is_valid_word(word):
                continue
            if re.search(r'[a-zA-Z]', word):
                # 翻译英文单词为中文
                final_words.append(word)
                translated_word = translate_to_chinese(word)
                if self._is_valid_word(translated_word):
                    final_words.append(translated_word)
            else:
                final_words.append(word)

        final_words = strip_list(final_words)
        logging.info(f"{reason_str} 最终分词结果是:{final_words}")

        # 处理自定义板块逻辑
        for custom_concept, keywords in self.custom_concept_words.items():
            for keyword in keywords:
                if any(word in keyword for word in final_words):
                    related_custom_concepts.add(custom_concept)

        final_words = strip_list(final_words)
        logging.info(f"{reason_str} 最终分词结果是: {final_words}")

        if not final_words and not related_custom_concepts:
            return None

        # 查询 ConceptInfo 数据表中符合条件的板块
        conditions = target_conditions[:]

        # 动态构造查询条件，适配 final_words 的实际长度
        like_conditions = [ConceptInfo.name.like(f"%{word}%") for word in final_words]
        conditions.append(or_(*like_conditions))

        session = Session()
        try:
            # 查询数据库中的板块信息
            concept_infos = session.query(ConceptInfo).filter(*conditions).all()
            db_concepts = {concept.name for concept in concept_infos}
            logging.info(f"数据库匹配的板块: {list(db_concepts)}")

            # 合并自定义板块和数据库查询结果
            all_concepts = list(db_concepts.union(related_custom_concepts))
            logging.info(f"{reason_str} 可能的板块有: {all_concepts}。若非空，即将openai调用归因")

            return all_concepts if all_concepts else None
        except Exception as e:
            logging.error(f"涨停原因 {reason_str} 分词 查询板块失败: {e}")
            return None
        finally:
            session.close()

    def _analyze_concept_from_split_words(self, reason_str, words, enbale_search=False):
        pre_concept_name_list = self._query_concepts_from_split_words(reason_str, words)
        logging.info(f"预处理得到板块用于AI语言分析: {pre_concept_name_list}")
        concept_name_list = None

        if pre_concept_name_list:
            # 调用 OpenAI 接口查找相关性最高的板块名称
            result = analyze_related_concept_with_limit_reason(reason_str, pre_concept_name_list)
            concept_name_list = result.get("output", None)
            unknown_word = result.get("unknown", None)
            if not concept_name_list and unknown_word and enbale_search:
                # 处理陌生术语
                logging.info(f"{unknown_word} 是陌生术语，GPT无法识别。即将联网查询")
                search_result = search_word_from_bing(unknown_word)

                search_title_list = search_result.get("names")
                search_summary_list = search_result.get("snippets")
                
                pre_concept_names_from_search_title = []
                search_result = ""
                for title, summary in zip(search_title_list, search_summary_list):
                    words_from_search_title = jieba.lcut(title)
                    concept_names_temp = self._query_concepts_from_split_words(title, words_from_search_title)
                    if concept_names_temp:
                        pre_concept_names_from_search_title += concept_names_temp
                        search_summary_temp = f"""
    - {summary}
                        """
                        search_result +=  search_summary_temp
                
                if search_result:
                    pre_concept_name_list = strip_list(pre_concept_names_from_search_title)
                    search_result = f"{unknown_word}背景信息: {search_result}"
                    logging.info(f"{unknown_word}: 推理出可能性的板块有: {pre_concept_name_list}")
                    logging.info(f"{search_result}")
                    result = analyze_related_concept_with_limit_reason(reason_str, pre_concept_name_list, search_result)
                    concept_name_list = result.get("output")
            
            if concept_name_list:
                concept_name_list = self.refine_output_with_relationships(concept_name_list)
                logging.info(f"{reason_str} 经过关系过滤后的参考板块是 {concept_name_list}")

        concept_names_str = None
        pre_concept_names_str = None
        if pre_concept_name_list:
            pre_concept_names_str = ','.join(pre_concept_name_list)
        if concept_name_list:
            concept_names_str = ','.join(concept_name_list)

        self._save_reason_data(reason_str, pre_concept_names_str, concept_names_str)
        return concept_name_list

    def _save_reason_data(self, reason_str, pre_concept_names_str, concept_names_str):
        # 更新数据表
        session = Session()
        max_retries = 5  # 最大重试次数
        for attempt in range(max_retries):
            try:
                # 将输入参数与结果映射并记录进数据表 limit_reason_to_concept
                origin_mapping = session.query(LimitReasonToConcept).filter(
                    LimitReasonToConcept.limit_reason == reason_str
                ).first()
                if origin_mapping:
                    logging.info(f"{reason_str}: 原归因板块: {origin_mapping.concept_names} 替换成: {concept_names_str}")
                else:
                    logging.info(f"{reason_str}: 无原归因板块，新增归因板块: {concept_names_str}")
                new_mapping = LimitReasonToConcept(
                    limit_reason=reason_str,
                    pre_concept_names=pre_concept_names_str,
                    concept_names=concept_names_str
                )
                session.merge(new_mapping)
                session.commit()
                break  # 成功后退出循环
            except Exception as e:
                session.rollback()
                logging.error(f"Attempt {attempt + 1}: 更新涨停原因对应板块数据: {e}")
                if attempt < max_retries - 1:  # 非最后一次尝试失败后，短暂延迟
                    time.sleep(random.uniform(1, 3))  # 随机延迟1到3秒
                else:
                    logging.error("达到最大重试次数，未能成功更新数据库")
            finally:
                session.close()

    def _analyze_concept_from_reason_by_rule(self, reason):
        new_concept_name = None
        if reason in self.custom_concept_words:
            new_concept_name = reason
        elif reason in conceptManager.concept_info_cache:
            new_concept_name = reason
        elif (reason+"概念股") in conceptManager.concept_info_cache:
            new_concept_name = reason+"概念股"
        elif (reason+"概念") in conceptManager.concept_info_cache:
            new_concept_name = reason+"概念"
        elif (reason+"Ⅲ") in conceptManager.concept_info_cache:
            new_concept_name = reason+"Ⅲ"
        elif "智慧" in reason and reason.replace("智慧", "智能") in conceptManager.concept_info_cache:
            new_concept_name = reason.replace("智慧", "智能")

        new_concept_name_list = [new_concept_name] if new_concept_name else []
        if new_concept_name_list:
            logging.info(f"直接写入，无需AI分析 {reason} - {new_concept_name}")
            self._save_reason_data(reason, None, ','.join(new_concept_name_list))
            return new_concept_name_list
        return []

    def _analyze_concept_from_reason_by_AI(self, reason_str):
        jieba_words = jieba.lcut(reason_str)
        for word in jieba_words:
            word = word.strip()
                    
            for return_word in self.return_words:
                if return_word in word:
                    logging.info(f"涨停原因 {reason_str} 属于个股股性、复牌等行为，无助于归因板块")
                    return None

        concept_name_list = self._analyze_concept_from_split_words(reason_str, jieba_words, False)
        if not concept_name_list:
            if any(word in self.return_words for word in jieba_words):
                return concept_name_list
            
            # jieba分词无法找到板块,通过openAI进行分词
            openai_words = analyze_key_word_with_limit_reason(reason_str, jieba_words)
            if openai_words and concepts_equal(jieba_words, openai_words) is False:
                # openAI分词板块结果与jieba分词结果不一致，再次尝试归因
                concept_name_list = self._analyze_concept_from_split_words(reason_str, openai_words, True)
                if not concept_name_list:
                    # 再次联想
                    logging.info('首次联想没有归因出板块，集合所有失败的关键词二次联想')
                    new_words = analyze_key_word_with_limit_reason(reason_str, jieba_words+openai_words)
                    if new_words and  concepts_equal(new_words, openai_words) is False:
                        concept_name_list = self._analyze_concept_from_split_words(reason_str, new_words, True)

        return concept_name_list

    # 根据涨停原因字符串分析板块
    def analyze_concept_from_reason(self, reason_str):
        concept_result = self._analyze_concept_from_reason_by_rule(reason_str)
        if not concept_result:
            concept_result = self._analyze_concept_from_reason_by_AI(reason_str)
        return concept_result

    def force_reclaim_limit_concept(self):
        session = Session()
        try:
            result = session.query(LimitReasonToConcept).all()
            for record in result:
                reason = record.limit_reason
                self._analyze_concept_from_reason_by_rule(reason)
        except Exception as e:
            session.rollback()
            logging.info(f"更新涨停原因失败 {e}")
        finally:
            session.close()

    def _verify_concept_list_with_stock_code(self, input_concept_list):
        if not input_concept_list:
            return []
        
        input_concept_list = strip_list(input_concept_list)

        valid_concept_list = []
        for concept_name in input_concept_list:
            if concept_name in self.ignore_concepts:
                continue
            valid_concept_list.append(concept_name)
        return valid_concept_list
    
    def find_valid_related_concept(self, reason): 
        return self._verify_concept_list_with_stock_code(self._get_concept_from_reason(reason))

    def update_concept_from_reason(self):
        with Session() as session:
            subject_concepts = session.query(ConceptInfo).filter(
                ConceptInfo.is_available == 0,
                ConceptInfo.exchange == 'A',
                or_(
                    ConceptInfo.type.in_(['N', 'I']),
                    and_(
                        ConceptInfo.type == 'S',
                        ConceptInfo.name == '破净股'
                    )
                ),
                ConceptInfo.name.isnot(None),
                ConceptInfo.count.is_(None)
            ).all()
            invalid_name_list = []
            for record in subject_concepts:
                invalid_name_list.append(record.name)

            result = session.query(LimitReasonToConcept).filter(
                LimitReasonToConcept.pre_concept_names.isnot(None)
            ).all()
            
            for data in result:
                reason = data.limit_reason
                pre_concept_list = data.pre_concept_names.split(',')
                if pre_concept_list:
                    for name in pre_concept_list:
                        if name in invalid_name_list:
                            logging.info(f"{reason} AI分析板块之前有无效数据,重新分析: {pre_concept_list}")
                            self.analyze_concept_from_reason(reason)
                            break

    def replace_concept_from(self, from_word, to_word):
        with Session() as session:
            # key_concept = sys.argv[1]
            result = session.query(LimitReasonToConcept).filter(
                LimitReasonToConcept.concept_names.like(f"%{from_word}%"),
            ).all()
            for record in result:
                ori_concepts = record.concept_names.split(',')
                # 替换列表中的 from_word 为 to_word
                updated_concepts = [to_word if concept == from_word else concept for concept in ori_concepts]
                updated_concepts = strip_list(updated_concepts)
                new_concepot_str = ','.join(updated_concepts)
                logging.info(f"{record.limit_reason}: {record.concept_names} 改为 {new_concepot_str}")
                record.concept_names = new_concepot_str
            # 提交修改
            session.commit()
    
    def analysis_custom_concept_reason(self, custom_concept_name):
        reason_to_concepts = {}
        with Session() as session:
            if custom_concept_name in self.custom_concept_words:
                key_words = self.custom_concept_words[custom_concept_name]
                for like_word in key_words:
                    result = session.query(LimitReasonToConcept).filter(
                        LimitReasonToConcept.limit_reason.like(f"%{like_word}%")
                    ).all()
                    for record in result:
                        if record.concept_names:
                            concepts = record.concept_names.split(',')
                            # if len(concepts) >1 and concepts[0] != key_concept:
                            # if len(concepts) == 3:
                            reason_to_concepts[record.limit_reason] = concepts
                        else:
                            reason_to_concepts[record.limit_reason] = None
        for reason, concepts in reason_to_concepts.items():
            logging.info(f"{reason} - {concepts}")
            reasonConceptManager.analyze_concept_from_reason(reason)

    def analysis_concept(self, concept_name):
        reason_to_concepts = {}
        with Session() as session:
            result = session.query(LimitReasonToConcept).filter(
                LimitReasonToConcept.concept_names.like(f"%{concept_name}%"),
                # LimitReasonToConcept.limit_reason.not_like(f"%设备%")
            ).all()
            for record in result:
                if record.concept_names:
                    concepts = record.concept_names.split(',')
                    # if len(concepts) >1:
                    # if len(concepts) == 3:
                    reason_to_concepts[record.limit_reason] = concepts
        for reason, concepts in reason_to_concepts.items():
            logging.info(f"{reason} - {concepts}")
            reasonConceptManager.analyze_concept_from_reason(reason)

    def refine_output_with_relationships(self, output):
        if not output or len(output) == 1:
            return output

        output_set = set(output)
        visited = set()
        chains = []

        for concept in output:
            if concept not in visited:
                chain = set()
                self.build_chain(concept, chain, visited)
                # Filter the chain to only include nodes in the output
                filtered_chain = chain & output_set
                if filtered_chain:
                    chains.append(filtered_chain)
                    logging.info(f"构建链条关系 '{concept}': {filtered_chain}")

        if len(chains) == 1:
            leaf_nodes = self.find_leaf_nodes(chains[0])
            logging.info(f"所有叶子结点板块: {leaf_nodes}")
            if len(leaf_nodes) > 1:
                root_node = self.find_root_node(chains[0], leaf_nodes)
                logging.info(f"存在多个叶子结点,寻找最近公共节点: {root_node}")
                return [root_node] if root_node else list(leaf_nodes)
            else:
                return list(leaf_nodes)

        # For multiple chains, find the lowest common ancestor
        all_nodes = set().union(*chains)
        lca = self.lowest_common_ancestor(list(all_nodes))
        if lca:
            logging.info(f"Found LCA for multiple chains: {lca}")
            return [lca]

        # If no common ancestor, return leaf nodes of each chain
        leaf_nodes = set()
        for chain in chains:
            chain_leaf_nodes = self.find_leaf_nodes(chain)
            logging.info(f"链条内的叶子节点 {chain}: {chain_leaf_nodes}")
            leaf_nodes.update(chain_leaf_nodes)
        return list(leaf_nodes)

    def build_chain(self, node, chain, visited):
        chain.add(node)
        visited.add(node)
        for child in conceptRelations.concept_to_descendants.get(node, []):
            if child not in visited:
                self.build_chain(child, chain, visited)
        for parent in conceptRelations.concept_to_category.get(node, []):
            if parent not in visited:
                self.build_chain(parent, chain, visited)

    def find_leaf_nodes(self, chain):
        leaf_nodes = set()
        for node in chain:
            children = conceptRelations.concept_to_descendants.get(node, [])
            if not children or not set(children) & chain:
                leaf_nodes.add(node)
        return leaf_nodes

    def find_root_node(self, chain, leaf_nodes):
        lca = self.lowest_common_ancestor(list(leaf_nodes))
        return lca

    def lowest_common_ancestor(self, nodes):
        ancestor_lists = []
        for node in nodes:
            # 获取每个节点的祖先链，包括自身
            ancestors = [node] + conceptRelations.concept_to_chains.get(node, [])
            ancestor_lists.append(ancestors)
        
        # 找到所有节点祖先链的交集
        common_ancestors = set(ancestor_lists[0])
        for ancestors in ancestor_lists[1:]:
            common_ancestors &= set(ancestors)
        
        if not common_ancestors:
            return None
        
        # 从共同祖先中，找到深度最小的节点（即距离当前节点最近的祖先）
        lca = None
        min_depth = float('inf')
        for ancestor in common_ancestors:
            # 计算每个节点到该祖先的深度
            depths = []
            for ancestors in ancestor_lists:
                if ancestor in ancestors:
                    depth = ancestors.index(ancestor)
                    depths.append(depth)
            # 取所有节点到该祖先的最大深度
            depth = max(depths)
            if depth < min_depth:
                min_depth = depth
                lca = ancestor
        return lca

    
reasonConceptManager = ReasonConceptManger()

if __name__ == "__main__":
    import sys
    reasonConceptManager.analyze_concept_from_reason(sys.argv[1])
    # reasonConceptManager.replace_concept_from(sys.argv[1], sys.argv[2])
    # reasonConceptManager.analysis_custom_concept_reason(sys.argv[1])
    # print(reasonConceptManager.custom_concept_words)

    # with Session() as session:
    #     result = session.query(LimitReasonToConcept).filter(
    #         LimitReasonToConcept.concept_names == None
    #     ).all()
    #     for record in result:
    #         print(record.limit_reason)

    # 测试数据
    # test_cases = {
    #     "case1": ["算力", "液冷服务器", "AIGC概念"],  # 单条链
    #     "case2": ["大消费", "饮料制造", "算力"],  # 多条不相交链
    #     "case3": ["数据要素", "算力", "AIGC概念"]   # 多条相交链
    # }

    # # 测试结果验证
    # for case_name, output in test_cases.items():
    #     logging.info(f"Testing {case_name} with output={output}")
    #     refined_output = reasonConceptManager.refine_output_with_relationships(output)
    #     logging.info(f"Refined Output: {refined_output}")

