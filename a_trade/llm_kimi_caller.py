import os
import logging
import base64
import a_trade.settings
from openai import OpenAI
from a_trade.llm_caller_base import LLMCaller

class KimiCaller(LLMCaller):
    def __init__(self):
        """
        初始化 KimiCaller，设置 OpenAI 客户端和模型信息。
        """
        self.visual_model = "moonshot-v1-8k-vision-preview"  # 替换为您的模型ID

        # 初始化 OpenAI 客户端
        self.client = OpenAI(
            api_key=os.getenv("KIMI_API_KEY"),
            base_url="https://api.moonshot.cn/v1",
        )

    def call_text_model_api(self, system_prompt: str, user_prompt: str = None, temperature: float = 0.1, max_tokens: int = 2000) -> str:
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
        :param image_path: 图片路径
        :param temperature: 生成的温度参数
        :param max_tokens: 最大生成的token数
        :return: 模型的响应文本
        """
        if not image_path:
            logging.error("image_path为空。")
            return ""
        
        if not os.path.exists(image_path):
            logging.error(f"文件 '{image_path}' 不存在。")
            return ""
        
        def encode_image(image_path):
            """
            将图片文件编码为Base64字符串。
            
            :param image_path: 图片路径
            :return: Base64编码的字符串
            """
            try:
                with open(image_path, "rb") as image_file:
                    return base64.b64encode(image_file.read()).decode('utf-8')
            except Exception as e:
                logging.error(f"图片编码失败: {e}")
                return ""

        base64_image = encode_image(image_path)
        if not base64_image:
            logging.error("图片编码失败。")
            return ""
        
        # 构建 Base64 编码的图片URL
        image_url = f"data:image/{os.path.splitext(image_path)[1]};base64,{base64_image}"
        
        messages = [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": image_url
                        }
                    },
                ],
            }
        ]
        
        try:
            logging.debug(f"发送的messages: {messages}")
            response = self.client.chat.completions.create(
                model=self.visual_model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                response_format={"type": "json_object"}
            )
            output = response.choices[0].message.content.strip()
            logging.info(f"响应: {output}")
            return output
        except Exception as e:
            logging.error(f"Kimi 多模态API调用失败: {e}")
            # 如果有错误响应内容，打印出来
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_content = e.response.json()
                    logging.error(f"错误响应内容: {error_content}")
                except Exception:
                    logging.error("无法解析错误响应内容。")
            raise