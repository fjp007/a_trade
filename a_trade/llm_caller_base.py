
# llm_caller_base.py
from abc import ABC, abstractmethod

class LLMCaller(ABC):
    @abstractmethod
    def call_text_model_api(
        self,
        system_prompt: str,
        user_prompt: str = None,
        temperature: float = 0.1,
        max_tokens: int = 2000
    ) -> str:
        """
        调用文本模型API并返回响应文本。

        :param system_prompt: 系统提示词
        :param user_prompt: 用户提示词
        :param temperature: 生成的温度参数
        :param max_tokens: 最大生成的token数
        :return: 模型的响应文本
        """
        pass

    @abstractmethod
    def call_visual_model_api(
        self,
        system_prompt: str,
        image_path: str,
        temperature: float = 0.1,
        max_tokens: int = 2000
    ) -> str:
        """
        调用多模态模型API并返回响应文本。

        :param system_prompt: 系统提示词
        :param image_path: 图片d
        :param temperature: 生成的温度参数
        :param max_tokens: 最大生成的token数
        :return: 模型的响应文本
        """
        pass
