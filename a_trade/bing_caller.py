import logging
import requests
import os
import a_trade.settings

subscription_key = os.getenv('BING_SEARCH_V7_SUBSCRIPTION_KEY')
endpoint = os.getenv('BING_SEARCH_V7_ENDPOINT') + "/v7.0/search"

def search_word_from_bing(word: str) -> dict[str, list[str]]:
    """
    使用 Bing 搜索并返回前三条结果的 'name' 和 'snippet'。
    
    :param word: 搜索关键词
    :return: 包含 'names' 和 'snippets' 两个数组的 JSON 对象
    """
    query = f'"{word}" (公司 OR 产品 OR 新兴领域 OR 近三年IP)'
    logging.info(f"Bing搜索词: {query}")
    mkt = 'zh-CN'
    params = { 'q': query, 'mkt': mkt }
    headers = { 'Ocp-Apim-Subscription-Key': subscription_key }

    try:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        # 提取搜索结果
        web_pages = response.json().get("webPages", {}).get("value", [])
        
        # 动态处理循环范围
        names = []
        snippets = []
        for page in web_pages[:min(len(web_pages), 3)]:
            name = page.get("name", "").strip()
            snippet = page.get("snippet", "").strip()
            if name:
                names.append(name)
            if snippet:
                snippets.append(snippet)
         # 返回结果 JSON
        result_json = {"names": names, "snippets": snippets}
        return result_json
    
    except requests.exceptions.RequestException as ex:
        logging.error(f"Bing API 请求失败: {ex}")
        return {"names": [], "snippets": []}
    except Exception as ex:
        logging.error(f"处理 Bing 搜索结果时出错: {ex}")
        return {"names": [], "snippets": []}