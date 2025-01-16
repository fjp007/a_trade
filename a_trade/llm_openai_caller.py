# llm_openai_caller.py
import os
import httpx
import logging
import a_trade.settings
from openai import OpenAI
from a_trade.llm_caller_base import LLMCaller

class OpenAICaller(LLMCaller):
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        proxy_http_url = os.getenv('PROXY_HTTP')
        
        if proxy_http_url:
            self.client = OpenAI(
                api_key=self.api_key,
                http_client=httpx.Client(
                    proxy=proxy_http_url,
                    transport=httpx.HTTPTransport(local_address="0.0.0.0"),
                    verify=False
                )
            )
        else:
            self.client = OpenAI(
                api_key=self.api_key
            )
    def call_text_model_api(self, system_prompt: str, user_prompt: str = None, temperature: float = 0, max_tokens: int = 2000) -> str:
        try:
             # 初始化消息列表，包含系统提示
            messages = [
                {"role": "system", "content": system_prompt}
            ]
            
            # 如果用户提示不为空，则添加到消息列表中
            if user_prompt:
                messages.append({"role": "user", "content": user_prompt})
            
            response = self.client.chat.completions.create(
                messages=messages,
                model="gpt-4o",
                max_tokens=max_tokens,
                temperature=temperature
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logging.error(f"OpenAI API调用失败: {e}")
            raise

    def call_visual_model_api(
        self,
        system_prompt: str,
        image_path: str,
        temperature: float = 0.1,
        max_tokens: int = 2000
    ) -> str:
        pass