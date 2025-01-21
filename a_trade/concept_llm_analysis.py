# coding: utf-8
# concept_analysis.py

import os
import logging
import a_trade.settings
import ast
import json
from a_trade.llm_caller_factory import LLMCallerFactory
from dotenv import load_dotenv
# 获取环境变量
openai_secret_key = os.getenv("OPENAI_API_KEY")
proxy_http_url = os.getenv('PROXY_HTTP')

# 加载环境变量
load_dotenv()

def analyze_key_word_with_limit_reason(limit_reason, jieba_words):
    logging.info(f"即将openai调用对{limit_reason}进行联想与分词")
    """
    调用 OpenAI GPT-4o 获取涨停原因联想分词
    """
    system_prompt = f"""
我将为你提供一只股票的涨停原因。请你为我通过总结+联想的方式找到涨停原因的关键词数组。我希望这个关键词数组可以有助于我找到涨停原因背后的板块名称。
**规则: **
 1. 数组里的每个关键词不超过4个字，数组元素个数最多4个。我会为你总结找到关键词数组的策略
 2. 一定要严格按照示例里的输出格式向我提供结果，无需解释原因。输出格式是可以正确被Python里ast.literal_eval正确解析成数组的字符串，数组外不要有多余字符，否则会解析失败。

**策略: **
 1. 先从涨停原因里总结出1-2个关键字，可以参考案例1
 2. 涨停原因如果包含类似'资产重组'、'收购'、'股份出售'、'股份变更'、'股权变更'的关键词，输出关键词一定要包含'并购重组'。这个策略的优先级相当高!请参考案例7
 3. 我会向你提供该涨停原因我认为无法帮助我分析板块的关键词数组。你输出请排除失败关键词数组中的任何一个关键字。如果你的总结里也包含了失败关键词。不妨通过策略4-策略7以及强制联想,找到更有效的关键词。
 4. 策略1直接总结出的关键词可能无法让我直接找到相应的板块，你可以考虑联想该关键词的从属关系。可以参考案例2。'苹果汁'是直接总结提取的关键字，'饮料'是'苹果汁'的从属关系。
 5. 策略1直接总结出的关键词可能无法让我直接找到相应的板块，你可以考虑联想该关键词存在整体与部分的关系。可以参考案例3，'汽车'与'安全气囊'是整体与部分的关系。。
 6. 涨停原因如果是具体的产品。请从背后可能关联的上级概念或相关产业/公司联想，避免使用原因中直接的词汇，而是寻找与之相关的更广泛概念。可以参考案例4与案例5
 7. 强制联想给出了多组(关键词1: 关键词2)的对应关系。策略1直接总结出的关键词如果包含关键词1，需要格式化成关键词2。请参考案例6

**强制联想: **
四川: 西部
字节: 抖音
电影: 影视
通讯: 通信
二胎: 三胎
再生: 节能环保

案例1: 
涨停原因: 园林景观
我期待你输出: ['园林', '景观']

案例2
涨停原因: 浓缩苹果汁
我期待你输出: ['苹果汁','饮料']

案例3
涨停原因: 安全气囊
我期待你输出: ['汽车', '零部件']

案例4
涨停原因: 荣耀
我期待你输出: ['手机', '华为']

案例5
涨停原因: Pika
我期待你输出: ['人工智能']

案例6
涨停原因: 电影全产业链
失败关键词: ['电影']
我期待你输出: ['影视']

案例7
涨停原因: 拟收购北京SKP旗下资产
我期待你输出: ['重组']
"""
    

    # 将分析数据作为用户输入
    user_prompt = f"""
请为我分析下面的数据:
涨停原因: {limit_reason}
失败关键词: {jieba_words}
"""
    
    # 获取 LLM 调用器，默认使用 OpenAI
    caller = LLMCallerFactory.get_caller("openai")
    
    # 调用文本模型API
    try:
        response_text = caller.call_text_model_api(
            system_prompt=system_prompt,
            user_prompt=user_prompt
        )
    except Exception as e:
        logging.error(f"文本API调用失败: {e}")
        raise
    
    try:
        key_word_list = ast.literal_eval(response_text)
        logging.info(f"涨停原因 '{limit_reason}' 通过 OpenAI 联想分词结果: {key_word_list}")
        return key_word_list
    except (ValueError, SyntaxError) as e:
        # OpenAI返回格式偶尔不受控，重试
        logging.error(f"LLM 联想分词解码错误: {e}, 返回文本是 {response_text}")
        # 可以选择重新调用或其他错误处理方式，这里选择重新调用
        return analyze_key_word_with_limit_reason(limit_reason, jieba_words)

def analyze_related_concept_with_limit_reason(limit_reason, concept_names, search_content=None):
    if not concept_names:
        return {"output": []}
    """
    调用 OpenAI GPT-4o 获取涨停原因联想分词
    """
    # 如果 search_content 非空，则补充背景信息
    background_info = search_content if search_content else ""
    system_prompt = f"""
我将为你提供一只股票的涨停原因，以及该股票可能的板块名称数组、可能还会提供涨停原因里陌生术语的背景信息。请你为我从数组范围内找到与涨停原因具有相关性的板块名称。
**重要指示: **
- 输出结果必须是一个 **纯粹的 JSON 字符串**。
- 严禁使用 Markdown 格式化（如 \`\`\`json 或 \`\`\`）。
- **输出内容必须从 '{{' 开始，直到 '}}' 结束**。任何额外的字符（如换行、注释或代码块标记）都将导致解析失败。

**规则: **
1. 板块名称数组里与涨停原因不相关的板块名称请丢弃。
2. 如果具有**直接相关性**的板块个数大于1，请确保输出数组元素是按照与涨停原因相关性从大到小的顺序组织的
3. 如果具有**直接相关性**的板块中存在多个。需要区分彼此之间的关系是包含关系还是并列关系，如果存在包含关系的多个板块，确保只返回在包含关系里最小的板块。比如人工智能包含AIGC概念，且都与涨停原因相关，只返回AIGC概念。
3. 输出结果的 JSON 结构如下:
    - 'output': 一个数组，数据只来源于板块名称数组，分析结果仅只提取**与涨停原因最核心、最直接相关的主要板块**，忽略包含其他细分领域或泛指的板块。若结果有多个，数组元素按照相关性排序。如果没有相关板块，则输出空数组。
    - 'reason': 推理过程，描述为什么选择这些板块名称，或者为什么没有相关板块。
    - 'unknown': 一个字符串，所有被认为是陌生术语或不常见术语的内容。如果没有陌生术语，请不要包含'unknown'字段。
4. 如果涨停原因包含了你没有直接知识或不常见的术语名称，必须将这些术语添加到 `unknown` 字段中，无论是否提到其模糊性。
5. 当我提供了背景信息的前提下，你的分析结果output是个空数组(即我提供的板块数组里没有与涨停原因与背景信息相关的板块)，你可以尝试结合背景信息推理一到多个均不超过4个字的板块名称放到output数组里。推理结果可以是相关行业。
6. **仅包含直接对应或同义的板块名称**：仅当板块名称与涨停原因直接对应或是同义词时，才将其包含在 output 数组中。避免包含仅有部分关联或泛指的板块名称。

**案例1**
涨停原因: 华为云
板块名称数组: [华为概念, 华为汽车, 云计算, 国资云, 云游戏, 云南]
我期待你输出: {{"output": "['云计算', '华为概念']", "reason": "推理过程"}}

**案例2**
涨停原因: 实控人已变更为国资
板块名称数组: [国资云]
我期待你输出: {{"output": "[]", "reason": "推理过程"}}

**案例3**
涨停原因: Kimi
板块名称数组: [区块链, 人工智能, 金融科技]
我期待你输出: {{"output": "[]", "reason": "推理过程", "unknown": "Kimi"}}
"""
    user_prompt = f"""
请为我分析下面的数据:
涨停原因: {limit_reason}
板块名称数组: {concept_names}
{background_info}
    """
    # 获取 LLM 调用器，默认使用 OpenAI
    caller = LLMCallerFactory.get_caller("openai")
    
    # 调用文本模型API
    try:
        response_text = caller.call_text_model_api(
            system_prompt=system_prompt,
            user_prompt=user_prompt
        )
    except Exception as e:
        logging.error(f"文本API调用失败: {e}")
        raise
    
    try:
        response_json = json.loads(response_text)
        concept_name_list = response_json.get("output", [])
        reason = response_json.get("reason", "无推理过程提供")
        unknown_word = response_json.get("unknown", None)
        
        if isinstance(concept_name_list, str):
            concept_name_list = ast.literal_eval(concept_name_list)
        
        logging.info(f"涨停原因 '{limit_reason}' 按相关性排序的板块名称是: {concept_name_list}")
        if not concept_name_list:
            logging.info(f"推理过程: {reason}")
        
        return {
            "output": concept_name_list,
            "reason": reason,
            "unknown": unknown_word
        } if unknown_word else {
            "output": concept_name_list,
            "reason": reason
        }
    except (ValueError, SyntaxError, json.JSONDecodeError) as e:
        logging.error(f"LLM 板块归因解码错误: {e}, JSON字符串是 {response_text}")
        return analyze_related_concept_with_limit_reason(limit_reason, concept_names, search_content)


def analyze_concept_datas_from_media_image(image_path):
    prompt = """
图片里是股票分类表。
**表格格式: **
表格第一行第一列是交易日，后面几列是合并单元格，单元格里是 一级分类(股票数量)(分类成交额)
表格第二行是表格的列名，我想让你分析的列名是股票代码、股票名称、概念。如果没有找到概念这个列名并且第一列没有列名，那第一列就是概念。

请你提取表格里的所有数据，转化成JSON数据。
**JSON数据结构示例: **
{
    "一级分类名":{
        "概念1":{
            "股票代码1": "股票名称1",
            "股票代码2": "股票名称2"
        },
        "概念2":{
            "股票代码3": "股票名称3",
            "股票代码4": "股票名称4"
        }
    }
}

**输出规则: **
1. 一级分类名取表格第一行第二列合并单元格中括号外的内容
2. 对于概念在第一列的表格，如果一行股票数据在当前行没有找到概念数据，应该采用以下规则得到概念数据:
	2.1 优先从之前行里有概念列不为空的行里获取概念作为当前行概念，例如表格里第1行概念列为新能源汽车，第2、3、4行概念列为空，第5行为光伏、第6行为空。则第1-4行概念判定为新能源汽车，第5-6行概念判定为光伏
	2.2 如果2.1没有得到有效概念数据，采用当前表格的一级分类名作为概念
3.绝对禁止将其他列名数据（如“主营描述”）作为概念
4.如果该表格概念不在第一列，并且表格里其他列也不存在明确的列名”概念”，采用当前的表格的一级分类名作为概念。
5.如果图片里不存在表格，输出空JSON
6.**输出内容必须从 '{{' 开始，直到 '}}' 结束**。任何额外的字符（如换行、注释或代码块标记）都将导致解析失败
"""
    # 获取 LLM 调用器，默认使用 Deepseek
    caller = LLMCallerFactory.get_caller("doubao")
    
    try:
        response_text = caller.call_visual_model_api(
            system_prompt=prompt,
            image_path=image_path,
            temperature=0,
        )
        response_json = json.loads(response_text)
        return response_json
    except (ValueError, SyntaxError, json.JSONDecodeError) as e:
        # OpenAI返回格式偶尔不受控，重试或调用备用API
        logging.error(f"Json解析错误: {e}, JSON字符串是 {response_text}")
        # 假设有一个备用函数 get_concept_datas_from_media_png_by_API
        return analyze_concept_datas_from_media_image(image_path)
    
if __name__ == "__main__":
    analyze_related_concept_with_limit_reason('磁性元器件', ['通信网络设备及器件','分立器件','磁性材料'])