# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import time

from scrapy import signals


class EiaProSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class EiaProDownloaderMiddleware:
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


from scrapy import signals
import scrapy
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class EIASpiderMiddleware(object):
    def process_request(self, request, spider):
        if spider.frequency == 'D':
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # 使用无头谷歌浏览器模式
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            # 指定谷歌浏览器路径
            self.driver = webdriver.Chrome(chrome_options=chrome_options, executable_path='C:\Virtualenvs\EIA\Lib\site-packages\selenium\webdriver\chrome\chromedriver.exe')
            self.driver.get(request.url)
            time.sleep(5)
            self.driver.execute_script("window.scrollTo(100, document.body.scrollHeight);")
            html = self.driver.page_source
            self.driver.quit()
            return scrapy.http.HtmlResponse(url=request.url, body=html.encode('utf-8'), encoding='utf-8',
                                                request=request)
        else:
            pass

import logging
import redis
import random
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware


USER_AGENT_LIST = [
    # Opera
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)'
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
        return request


from twisted.internet import defer
from twisted.internet.error import TimeoutError, DNSLookupError, \
    ConnectionRefusedError, ConnectionDone, ConnectError, \
    ConnectionLost, TCPTimedOutError
from twisted.web.client import ResponseFailed
from scrapy.core.downloader.handlers.http11 import TunnelError

class MyIPProxyMiddleWare(object):
    ALL_EXCEPTIONS = (defer.TimeoutError, TimeoutError, DNSLookupError,
                      ConnectionRefusedError, ConnectionDone, ConnectError,
                      ConnectionLost, TCPTimedOutError, ResponseFailed,
                      IOError, TunnelError)

    '''
    ip 代理池
    '''

    def process_request(self, request, spider):
        # 从list中选取IP，设置到request
        ip_proxy = self.IP_PROXY_LIST()
        if ip_proxy:
            request.meta['proxies'] = ip_proxy  # 此处关键字proxies不能错

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        # 如果返回的response状态不是200，重新生成当前request对象
        if response.status != 200:
            # 保存
            name = time.strftime('%Y-%m-%d %H:%M', time.localtime())
            with open(str(name), 'a+') as file:
                file.write(response.url)

            proxy = self.IP_PROXY_LIST()
            # 对当前reque加上代理
            request.meta['proxy'] = proxy
            return request
        return response

    def process_exception(self, request, exception, spider):
        error_info = f"spider:{spider.name} MyIPProxyMiddleWare has error with {exception}"
        print(error_info)
        logging.error(error_info)

        if isinstance(exception, self.ALL_EXCEPTIONS):
            # 在日志中打印异常类型
            print('Got exception: %s' % (exception))
            return request
        print('not contained exception: %s' % exception)


    def IP_PROXY_LIST(self):
        conn = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        IP_PROXY_LIST = conn.hkeys('useful_proxy')
        ip_proxy = random.choice(IP_PROXY_LIST)
        return ip_proxy