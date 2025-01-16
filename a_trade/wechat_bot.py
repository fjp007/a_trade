# coding: utf-8
# wechat_bot.py
import requests
import base64
import hashlib
import os
import logging
from typing import List, Tuple, Union
import a_trade.settings
from a_trade.trade_utils import strip_code_suffix

proxy_http_url = os.getenv('PROXY_HTTP')
proxy_https_url = os.getenv('PROXY_HTTPS')

wechat_bot_url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=02951934-436a-4aa8-b145-bff2f24c43f5'

class WechatBot:
    @staticmethod
    def send_observe_pool_msg(
        trade_date: str,
        buy_stocks: Union[List[Union[str, Tuple]], Tuple[Union[str, Tuple], ...]],
        sell_stocks: Union[List[Union[str, Tuple]], Tuple[Union[str, Tuple], ...]]
    ) -> None:
        """
        发送观察池消息

        :param trade_date: str, 交易日期字符串，用于标识消息的日期
        :param buy_stocks: list或tuple, 买入股票观察池，
                        每个元素类似 (股票代码, 股票名称, concept_position, daily_position, is_t_limit)
        :param sell_stocks: list或tuple, 卖出股票观察池，
                        每个元素类似 (股票代码, 股票名称)
        """
        # 将 buy_stocks 整理成文字
        buy_text_lines: List[str] = []
        for stock in buy_stocks:
            if isinstance(stock, (list, tuple)):
                # 解构 stock 信息
                stock_code: str = stock[0]
                stock_name = stock[1]
                concept_name = stock[2]
                concept_position = stock[3]
                is_t_limit = stock[4]
                
                t_text = "[T字板]" if is_t_limit else ''
                # 这里可根据实际业务需求进行格式化、拼装
                buy_text_lines.append(
                    f"{strip_code_suffix(stock_code)} - {stock_name} - "
                    f"[{concept_name} {concept_position}]"
                    f"{t_text}"
                )
            else:
                # 如果不是 tuple，就当作纯股票代码处理
                buy_text_lines.append(str(stock))
        
        # 将 sell_stocks 整理成文字
        sell_text_lines: List[str] = []
        for stock in sell_stocks:
            if isinstance(stock, (list, tuple)):
                stock_code: str = stock[0]
                # 可根据需要格式化更多信息，例如股票名称等
                sell_text_lines.append(f"{strip_code_suffix(stock_code)} - {' '.join(map(str, stock[1:]))}")
            else:
                stock_code: str = stock
                sell_text_lines.append(stock_code)
        
        # 拼装消息文本
        msg_lines: List[str] = []
        msg_lines.append(f"【{trade_date}观察池】")  # 添加交易日期
        msg_lines.append("=== 买入观察池 ===")
        if buy_text_lines:
            msg_lines.extend(buy_text_lines)
        else:
            msg_lines.append("无")
        
        msg_lines.append("\n=== 卖出观察池 ===")
        if sell_text_lines:
            msg_lines.extend(sell_text_lines)
        else:
            msg_lines.append("无")
        
        final_msg: str = "\n".join(msg_lines)
        WechatBot.send_msg("text", final_msg)
    
    @staticmethod
    def send_stop_subscribe_msg(stock_code: str) -> None:
        """
        发送股票停止监听消息
        :param stock_code:  str, 股票代码
        :param buy_price:   float, 买入价格
        :return: None
        """
        final_msg = (
            f"【停止监听】\n"
            f"股票代码: {strip_code_suffix(stock_code)}\n"
        )
        return WechatBot.send_msg("text", final_msg)

    @staticmethod
    def send_buy_stock_msg(stock_code: str, stock_name: str, buy_price: float, buy_time_str: str) -> None:
        """
        发送买入股票消息
        :param stock_code:  str, 股票代码
        :param buy_price:   float, 买入价格
        :param buy_time_str: str, 买入时间 (格式自定义, 例如 '2025-01-10 09:31:00')
        :return: None
        """
        final_msg = (
            f"【买入通知】\n"
            f"股票代码: {strip_code_suffix(stock_code)}\n"
            f"股票名称: {stock_name}\n"
            f"买入价格: {buy_price}\n"
            f"买入时间: {buy_time_str}"
        )
        return WechatBot.send_msg("text", final_msg)

    @staticmethod
    def send_sold_stock_msg(
        stock_code: str, 
        sold_price: float, 
        sold_time_str: str, 
        sell_reason: str = "未指定"
    ) -> None:
        """
        发送卖出股票消息
        :param stock_code: str, 股票代码
        :param sold_price: float, 卖出价格
        :param sold_time_str: str, 卖出时间 (格式自定义, 例如 '2025-01-10 14:55:00')
        :param sell_reason: str, 卖出原因 (默认值为 "未指定")
        :return: None
        """
        final_msg = (
            f"【卖出通知】\n"
            f"股票代码: {strip_code_suffix(stock_code)}\n"
            f"卖出价格: {sold_price if sold_price > 0 else '市价成交'}\n"
            f"卖出时间: {sold_time_str}\n"
            f"卖出原因: {sell_reason}"
        )
        WechatBot.send_text_msg(final_msg)
    
    @staticmethod
    def send_text_msg(msg: str) -> None:
        return WechatBot.send_msg("text", msg)
    
    @staticmethod
    def send_msg(msgtype: str, msg: str) -> None:
        """
        发送文本消息到企业微信
        :param msgtype: "text" 等
        :param msg: 要发送的文本内容
        """
        paras = {'msgtype': msgtype, msgtype: {'content': msg}}
        try:
            proxies = {
                "http": proxy_http_url,
                "https": proxy_https_url
            } if proxy_http_url else {}

            logging.info(f"代理: {proxy_http_url}" if proxy_http_url else "无代理")

            result = requests.post(wechat_bot_url, json=paras, proxies=proxies or None)
            if result.status_code == 200 and result.json().get('errcode') == 0:
                logging.info("微信消息发送成功！")
                return True
            else:
                logging.error(f"微信消息发送失败: {result.json()}")
                return False
        except Exception as e:
            logging.error(f"发送消息时发生异常: {e}")
            return False

    @staticmethod
    def send_image_msg(image_path: str) -> None:
        """
        发送图片消息
        :param image_path: str, 图片文件绝对路径
        """
        try:
            if not os.path.exists(image_path):
                logging.error(f"图片文件不存在: {image_path}")
                return False
            
            image_size = os.path.getsize(image_path)
            if image_size > 2 * 1024 * 1024:
                logging.error(f"图片大小为{image_size},超过2M，无法发送。")
                return False

            with open(image_path, "rb") as image_file:
                image_data = image_file.read()

            base64_data = base64.b64encode(image_data).decode('utf-8')
            md5_hash = hashlib.md5(image_data).hexdigest()

            paras = {
                "msgtype": "image",
                "image": {
                    "base64": base64_data,
                    "md5": md5_hash
                }
            }

            proxies = {
                "http": proxy_http_url,
                "https": proxy_https_url
            } if proxy_http_url else {}

            logging.info(f"代理: {proxy_http_url}" if proxy_http_url else "无代理")

            result = requests.post(wechat_bot_url, json=paras, proxies=proxies or None)

            if result.status_code == 200 and result.json().get('errcode') == 0:
                logging.info("微信图片消息发送成功！")
                return True
            else:
                logging.error(f"微信图片消息发送失败: {result.json()}")
                return False

        except Exception as e:
            logging.error(f"发送图片消息时发生异常: {e}")
            return False
        
if __name__ == '__main__':
    WechatBot.send_text_msg('测试消息')
