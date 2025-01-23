# coding: utf-8
# baidu_translate.py

import random
import hashlib
import urllib.parse
import requests
import logging
import a_trade.settings
import os

def translate_to_chinese(word: str) -> str:
    logging.info(f'百度翻译词语:{word}')
    # 使用百度翻译 API 实现翻译
    url = "https://fanyi-api.baidu.com/api/trans/vip/translate"
    appid = os.getenv("BAIDU_TRANSLATE_APP_ID")  
    secret_key = os.getenv("BAIDU_TRANSLATE_SECRET_KEY")
    salt = str(random.randint(32768, 65536))
    sign = hashlib.md5((appid + word + salt + secret_key).encode('utf-8')).hexdigest()

    params = {
        "q": urllib.parse.quote(word),
        "from": "en",
        "to": "zh",
        "appid": appid,
        "salt": salt,
        "sign": sign
    }

    try:
        response = requests.get(url, params=params)
        result = response.json()
        if "trans_result" in result:
            return result["trans_result"][0]["dst"]
        else:
            logging.error(f"百度翻译 API error: {result}")
            return word
    except requests.exceptions.RequestException as e:
        logging.error(f"百度翻译 API错误: {e}")
        return word
    
if __name__ == '__main__':
    print(translate_to_chinese('VR'))