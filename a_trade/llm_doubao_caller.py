
import os
from volcenginesdkarkruntime import Ark
import logging
from a_trade.llm_caller_base import LLMCaller
import base64
import json_repair

class DoubaoCaller(LLMCaller):
    def __init__(self):
        self.visual_model = "ep-20250114173836-nmzjv"

        self.client = Ark(
            base_url='https://ark.cn-beijing.volces.com/api/v3',
            api_key=os.getenv('DOUBAO_API_KEY'),
        )

    
    def call_text_model_api(
        self,
        system_prompt: str,
        user_prompt: str = None,
        temperature: float = 0.1,
        max_tokens: int = 2000
    ) -> str:
        pass
    
    def call_visual_model_api(
        self,
        system_prompt: str,
        image_path: str,
        temperature: float = 0.1,
        max_tokens: int = 2000
    ) -> str:
        """
        实现多模态模型API调用，发送文本和图片URL。

        :param system_prompt: 系统提示词
        :param user_prompt: 用户提示词，包括文本和图片URL（列表形式）
        :param temperature: 生成的温度参数
        :param max_tokens: 最大生成的token数
        :return: 模型的响应文本
        """
        if not image_path:
            return {}
        
        if not os.path.exists(image_path):
            logging.info("error：文件 '{image_path}' 不存在。")
            return {}
        
        def encode_image(image_path):
            with open(image_path, "rb") as image_file:
                return base64.b64encode(image_file.read()).decode('utf-8')
            
        def get_content_type(image_path):
            # 扩展名到 Content-Type 的映射表
            extension_to_content_type = {
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.png': 'image/png',
                '.apng': 'image/png',
                '.gif': 'image/gif',
                '.webp': 'image/webp',
                '.bmp': 'image/bmp',
                '.dib': 'image/bmp',
                '.tiff': 'image/tiff',
                '.tif': 'image/tiff',
                '.ico': 'image/x-icon',
                '.icns': 'image/icns',
                '.sgi': 'image/sgi',
                '.j2c': 'image/jp2',
                '.j2k': 'image/jp2',
                '.jp2': 'image/jp2',
                '.jpc': 'image/jp2',
                '.jpf': 'image/jp2',
                '.jpx': 'image/jp2',
            }
            
            # 获取文件扩展名
            import os
            _, ext = os.path.splitext(image_path)
            ext = ext.lower()
            
            # 查找 Content-Type
            return extension_to_content_type.get(ext, 'unknown')
            
        base64_image = encode_image(image_path)
        if not base64_image:
            return {}
        
        PREFILL_PRIFEX = "{"
        messages = [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [{
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:{get_content_type(image_path)};base64,{base64_image}"
                    }
                }],
            },
            {
                "role": "assistant",
                "content": PREFILL_PRIFEX
            }
        ]
        
        try:
            response = self.client.chat.completions.create(
                model=self.visual_model,
                messages=messages,
            )
            json_string = PREFILL_PRIFEX + response.choices[0].message.content.strip()
            logging.info(f"{json_string}")
            return json_string
        except Exception as e:
            logging.error(f"Doubao 多模态API调用失败: {e}")
            raise