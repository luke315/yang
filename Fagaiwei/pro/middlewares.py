# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import re
import time

from scrapy import signals

import logging
import redis
import random
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.http import HtmlResponse

from scrapy import signals
import scrapy
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class EIASpiderMiddleware(object):
    driver = None

    # def process_request(self, request, spider):
    #     if spider.name == 'American_stock' or spider.name == 'hk_stock':
    #         chrome_options = Options()
    #         chrome_options.add_argument('--headless')  # 使用无头谷歌浏览器模式
    #         chrome_options.add_argument('--disable-gpu')
    #         chrome_options.add_argument('--no-sandbox')
    #         # 指定谷歌浏览器路径
    #         self.driver = webdriver.Chrome(chrome_options=chrome_options, executable_path='C:\Virtualenvs\EIA\Lib\site-packages\selenium\webdriver\chrome\chromedriver.exe')
    #         self.driver.get(request.url)
    #         html = self.driver.page_source
    #         if spider.name == 'American_stock':
    #             id_type = '//*[@id="pages"]'
    #         else:
    #             id_type = '//*[@id="tbl_wrap"]/div'
    #         res = self.driver.find_element_by_xpath('%s/a[last()]' % id_type).text
    #         num = spider.num
    #         if spider.num != 1 and re.search('下一页', res):
    #             while num != 1:
    #                 time.sleep(0.5)
    #                 self.driver.find_element_by_xpath('%s/a[last()]' % id_type).click()
    #                 num = num - 1
    #             html = self.driver.page_source
    #             return scrapy.http.HtmlResponse(url=request.url, body=html.encode('utf-8'), encoding='utf-8',
    #                                             request=request)
    #         else:
    #             return scrapy.http.HtmlResponse(url=request.url, body=html.encode('utf-8'), encoding='utf-8',
    #                                         request=request)
    #     else:
    #         pass

    def process_response(self, request, response, spider):
        """
        三个参数:
        # request: 响应对象所对应的请求对象
        # response: 拦截到的响应对象
        # spider: 爬虫文件中对应的爬虫类 WangyiSpider 的实例对象, 可以通过这个参数拿到 WangyiSpider 中的一些属性或方法
        """

        #  对页面响应体数据的篡改, 如果是每个模块的 url 请求, 则处理完数据并进行封装
        if spider.name == 'hk_stock' or spider.name == 'American_stock' or spider.name == 'American_stock_info':
            if request.url in ['http://vip.stock.finance.sina.com.cn/mkt/#qbgg_hk', 'http://finance.sina.com.cn/stock/usstock/sector.shtml#cm']:
                spider.browser.get(url=request.url)
                # more_btn = spider.browser.find_element_by_class_name("post_addmore")     # 更多按钮
                # print(more_btn)
                js = "window.scrollTo(0,document.body.scrollHeight)"
                spider.browser.execute_script(js)
                # if more_btn and request.url == "http://news.163.com/domestic/":
                #     more_btn.click()
                time.sleep(1)     # 等待加载,  可以用显示等待来优化.
                row_response = spider.browser.page_source
                return HtmlResponse(url=spider.browser.current_url, body=row_response, encoding="utf8", request=request)   # 参数url指当前浏览器访问的url(通过current_url方法获取), 在这里参数url也可以用request.url
                                                                                                                           # 参数body指要封装成符合HTTP协议的源数据, 后两个参数可有可无
            else:
                return response
        else:
            return response


USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1',
    'Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6',
    'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5',
    'Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24',
    'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24',
    ]

class RotateUserAgentMiddleware(UserAgentMiddleware):
    # 创建ua
    def process_request(self, request, spider):
        user_agent = random.choice(USER_AGENT_LIST)
        if user_agent:
            request.headers.setdefault('User-Agent', user_agent)
        return None

    def process_exception(self, request, exception, spider):
        error_info = f"spider:{spider.name} RotateUserAgentMiddleware has error with {exception}"
        print(error_info)
        logging.error(error_info)

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




# your spider code
#
#
# class ProSpiderMiddleware:
#     # Not all methods need to be defined. If a method is not defined,
#     # scrapy acts as if the spider middleware does not modify the
#     # passed objects.
#
#     @classmethod
#     def from_crawler(cls, crawler):
#         # This method is used by Scrapy to create your spiders.
#         s = cls()
#         crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
#         return s
#
#     def process_spider_input(self, response, spider):
#         # Called for each response that goes through the spider
#         # middleware and into the spider.
#
#         # Should return None or raise an exception.
#         return None
#
#     def process_spider_output(self, response, result, spider):
#         # Called with the results returned from the Spider, after
#         # it has processed the response.
#
#         # Must return an iterable of Request, dict or Item objects.
#         for i in result:
#             yield i
#
#     def process_spider_exception(self, response, exception, spider):
#         # Called when a spider or process_spider_input() method
#         # (from other spider middleware) raises an exception.
#
#         # Should return either None or an iterable of Request, dict
#         # or Item objects.
#         pass
#
#     def process_start_requests(self, start_requests, spider):
#         # Called with the start requests of the spider, and works
#         # similarly to the process_spider_output() method, except
#         # that it doesn’t have a response associated.
#
#         # Must return only requests (not items).
#         for r in start_requests:
#             yield r
#
#     def spider_opened(self, spider):
#         spider.logger.info('Spider opened: %s' % spider.name)


class ProDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
