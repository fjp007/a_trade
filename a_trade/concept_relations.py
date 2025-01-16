# coding: utf-8
import json
from a_trade.settings import get_project_path

class ConceptRelations:
    def __init__(self):
        """
        初始化 ConceptRelations 类，加载板块关系数据并构建全局链条和反向索引
        :param concept_file: 包含板块关系的 JSON 文件路径
        """
        self.concept_to_category = {}      # 板块的父子关系
        self.concept_to_chains = {}        # 板块的完整链条
        self.concept_to_descendants = {}  # 板块的所有子板块

        concept_file_path = get_project_path() / 'concept_category.json'
        self._load_concept_infos(concept_file_path)

    def _load_concept_infos(self, concept_file_path):
        """
        加载板块数据并构建链条和反向索引
        """
        # 加载父子关系数据
        with open(concept_file_path, 'r', encoding='utf-8') as file:
            self.concept_to_category = json.load(file)

        # 构建完整链条
        for concept_name in self.concept_to_category.keys():
            if concept_name not in self.concept_to_chains:
                self.concept_to_chains[concept_name] = self._build_full_chain(concept_name)

        # 构建反向索引
        self.concept_to_descendants = self._build_descendants()

    def _build_full_chain(self, concept_name, visited=None):
        """
        优化：递归构建板块的完整父辈关系链条，确保按从子到父依次展开父级板块
        """
        if visited is None:
            visited = set()

        # 如果已访问过，避免循环依赖
        if concept_name in visited:
            return []

        # 标记当前板块为已访问
        visited.add(concept_name)

        # 初始化链条
        chain = []

        # 延迟递归处理父板块
        delayed_chain = []

        # 遍历所有直接父板块
        if concept_name in self.concept_to_category:
            for parent in self.concept_to_category[concept_name]:
                # 记录直接父板块
                chain.append(parent)
                # 延迟递归处理父板块的链条
                delayed_chain.extend(self._build_full_chain(parent, visited))

        # 合并直接父板块和递归链条，保持顺序
        chain.extend(delayed_chain)

        # 去重并保持顺序
        return list(dict.fromkeys(chain))

    def _build_descendants(self):
        """
        构建每个板块的所有子板块（直接和间接）
        """
        # 初始化父板块到子板块的映射
        parent_to_children = {}

        # 遍历子板块到父板块的映射，反向构建父板块到子板块的映射
        for child, parents in self.concept_to_category.items():
            for parent in parents:
                if parent not in parent_to_children:
                    parent_to_children[parent] = set()
                if parent != child:
                    parent_to_children[parent].add(child)

        # 确保所有板块（包括仅作为父板块的键）都在映射中
        all_concepts = set(self.concept_to_category.keys()).union(set(parent_to_children.keys()))
        descendants = {concept: set() for concept in all_concepts}

        def collect_descendants(parent):
            """
            递归收集某板块的所有子板块
            """
            if parent not in parent_to_children:
                return set()

            # 获取直接子板块
            direct_descendants = parent_to_children[parent]

            # 递归收集所有间接子板块
            for child in direct_descendants:
                descendants[parent].add(child)
                descendants[parent].update(collect_descendants(child))

            return descendants[parent]

        # 递归处理所有板块
        for concept in all_concepts:
            collect_descendants(concept)

        return descendants

    def get_related_concepts(self, concept_name):
        """
        获取目标板块的相关板块，包括父板块和子板块
        """
        if concept_name not in self.concept_to_chains and concept_name not in self.concept_to_descendants:
            return []

        # 父板块：从链条中找到目标板块的其他节点
        chain = self.concept_to_chains.get(concept_name, [])
        related_from_chain = [concept for concept in chain if concept != concept_name]

        # 子板块：从反向索引中找到所有直接和间接的子板块
        related_from_descendants = list(self.concept_to_descendants.get(concept_name, []))

        # 合并父板块和子板块
        return list(set(related_from_chain + related_from_descendants))

    def is_parent_concept(self, parent_concept, child_concept):
        """
        判断 parent_concept 是否是 child_concept 的父辈关系（包括父亲和祖先）
        :param parent_concept: 待判断的父辈板块名称
        :param child_concept: 待判断的子板块名称
        :return: True 如果 parent_concept 是 child_concept 的父辈，否则 False
        """
        if child_concept not in self.concept_to_chains:
            return False

        # 获取 child_concept 的完整父辈链条
        parent_chain = self.concept_to_chains[child_concept]

        # 检查 parent_concept 是否在链条中
        return parent_concept in parent_chain

conceptRelations = ConceptRelations()

if __name__ == "__main__":
    # print(relations.concept_to_chains)
    print(conceptRelations.concept_to_descendants)
    print(conceptRelations.get_related_concepts('华为概念'))
