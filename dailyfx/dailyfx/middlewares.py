# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import json
import logging
import random
import re
import time
import redis
import requests
from scrapy.http import HtmlResponse
import datetime
from lib.lib_args import url_list
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware


class InvestingSpiderMiddleware(object):
    # def process_request(self, request, spider):
    #     res1 = re.search('https://cn.investing.com/economic-calendar/', request.url)
    #     if res1 != None:
    #         # 如果有的请求是带有payload请求的，在这个里面处理掉
    #         if request.meta.get('payloadFlag', False):
    #             postUrl = request.url
    #             headers = request.meta.get('headers', {})
    #             payloadData = request.meta.get('payloadData', {})
    #             # 发现这个居然是个同步 阻塞的过程，太过影响速度了
    #             res = requests.post(postUrl, data=payloadData, headers=headers)
    #             print(res)
    #             if res.status_code > 199 and res.status_code < 300:
    #                 # 返回Response，就进入callback函数处理，不会再去下载这个请求
    #                 return HtmlResponse(url=postUrl,
    #                                     body=res.content,
    #                                     # 最好根据网页的具体编码而定
    #                                     encoding='utf-8',
    #                                     status=200,
    #                                     )
    #             else:
    #                 return HtmlResponse(url=request.url, status=500, request=request)

    def process_response(self, request, response, spider):
        """
        三个参数:
        # request: 响应对象所对应的请求对象
        # response: 拦截到的响应对象
        # spider: 爬虫文件中对应的爬虫类 WangyiSpider 的实例对象, 可以通过这个参数拿到 WangyiSpider 中的一些属性或方法
        """

        #  对页面响应体数据的篡改, 如果是每个模块的 url 请求, 则处理完数据并进行封装
        if re.search('https://cn.investing.com/search/', request.url) != None and spider.name == 'investing_com':
            spider.browser.get(url=request.url)
            # 滑到底部
            js = "window.scrollTo(0,document.body.scrollHeight)"
            spider.browser.execute_script(js)
            time.sleep(4)  # 等待加载,  可以用显示等待来优化.
            row_response = spider.browser.page_source
            return HtmlResponse(url=spider.browser.current_url, body=row_response, encoding="utf8",
                                request=request)  # 参数url指当前浏览器访问的url(通过current_url方法获取), 在这里参数url也可以用request.url
            # 参数body指要封装成符合HTTP协议的源数据, 后两个参数可有可无

        elif re.search('https://cn.investing.com/economic-calendar/', request.url) != None and spider.name == 'investing_com_T_selenium':
            spider.browser.get(url=request.url)
            # 滑到底部
            js = "window.scrollTo(0,document.body.scrollHeight)"
            spider.browser.execute_script(js)
            # time.sleep(4)  # 等待加载,  可以用显示等待来优化.
            row_response = spider.browser.page_source
            return HtmlResponse(url=spider.browser.current_url, body=row_response, encoding="utf8",
                                request=request)  # 参数url指当前浏览器访问的url(通过current_url方法获取), 在这里参数url也可以用request.url
        else:
            return response



USER_AGENT_LIST = [
    # Opera
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)'
    ]


class RotateUserAgentMiddleware(UserAgentMiddleware):
    # 创建ua
    def process_request(self, request, spider):
        if spider.name == 'investing_com':
            user_agent = random.choice(USER_AGENT_LIST)
            # user_agent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3314.0 Safari/537.36 SE 2.X MetaSr 1.0'
            if user_agent:
                request.headers.setdefault('User-Agent', user_agent)
            return None

    def process_exception(self, request, exception, spider):
        error_info = f"spider:{spider.name} RotateUserAgentMiddleware has error with {exception}"
        print(error_info)
        logging.error(error_info)
        return request


class MyIPProxyMiddleWare(object):
    '''
    ip 代理池
    '''

    def process_request(self, request, spider):
        pass
        # if spider.name == 'investing_com':
        #     ip_proxy = self.IP_PROXY_LIST()
        #     if ip_proxy:
        #         request.meta['proxies'] = ip_proxy  # 此处关键字proxies不能错

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        # 如果返回的response状态不是200，重新生成当前request对象
        if response.status != 200:
            # proxy = self.IP_PROXY_LIST()
            # # 对当前reque加上代理
            # request.meta['proxy'] = proxy
            return request
        return response

    def process_exception(self, request, exception, spider):
        error_info = f"spider:{spider.name} MyIPProxyMiddleWare has error with {exception}"
        print(error_info)
        logging.error(error_info)
        return request

    def IP_PROXY_LIST(self):
        conn = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        IP_PROXY_LIST = conn.hkeys('useful_proxy')
        ip_proxy = random.choice(IP_PROXY_LIST)
        return ip_proxy

