from a_trade.llm_caller_base import LLMCaller
from a_trade.llm_openai_caller import OpenAICaller
from a_trade.llm_doubao_caller import DoubaoCaller
from a_trade.llm_deepseek_caller import DeepseekCaller
from a_trade.llm_302ai_caller import TZTAICaller
from a_trade.llm_kimi_caller import KimiCaller
from a_trade.llm_ali_caller import ALICaller
import logging
import a_trade.settings

class LLMCallerFactory:
    @staticmethod
    def get_caller(model_type: str) -> LLMCaller:
        """
        根据模型类型返回相应的LLM调用器实例。

        :param model_type: 模型类型，如 'openai', 'doubao', 'deepseek'
        :return: LLMCaller的实例
        """
        if model_type.lower() == "openai":
            return OpenAICaller()
        elif model_type.lower() == "doubao":
            return DoubaoCaller()
        elif model_type.lower() == "deepseek":
            return DeepseekCaller()
        elif model_type.lower() == "kimi":
            return KimiCaller()
        elif model_type.lower() == 'ali':
            return ALICaller()
        elif model_type.lower() == '302ai':
            return TZTAICaller()
        else:
            logging.error(f"不支持的模型类型: {model_type}")
            raise ValueError(f"Unsupported model type: {model_type}")