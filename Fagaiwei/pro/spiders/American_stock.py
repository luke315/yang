# -*- coding: utf-8 -*-
import time
from scrapy.http import HtmlResponse

from lib.args.lib_args import current_time
from pro.items import American_Stock_Item
import scrapy
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class AmericanStockSpider(scrapy.Spider):
    name = 'American_stock'
    start_urls = ['http://finance.sina.com.cn/stock/usstock/sector.shtml#cm']
    num = 0
    response = ''

    custom_settings = {
        'ITEM_PIPELINES': {'pro.pipelines.American_Stock_Pipline': 300},

        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': '././Logs/%s.%s.log' % (name, current_time)
    }

    def __init__(self):
        # 实例化一个浏览器对象(实例化一次)
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # 使用无头谷歌浏览器模式
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        self.browser = webdriver.Chrome(chrome_options=chrome_options, executable_path='C:\Virtualenvs\EIA\Lib\site-packages\selenium\webdriver\chrome\chromedriver.exe')
        super().__init__()

    # 必须在整个爬虫结束后，关闭浏览器
    def closed(self, spider):
        print('爬虫结束')
        self.browser.quit()

    def parse(self, response):
        self.response = response
        page_num = self.browser.find_element_by_xpath('//*[@id="pages"]/a[last()-1]').text
        while int(page_num) > self.num:
            tr_list = self.response.xpath('//*[@id="data"]/table/tbody/tr')
            for tr in tr_list:
                stock_name = tr.css('::text').extract()[0]
                stock_code = tr.css('::text').extract()[1]
                latest_price = tr.css('::text').extract()[2]  # 最新价
                rise_fall = tr.css('::text').extract()[3]  # 涨跌额
                applies = tr.css('::text').extract()[4]  # 涨跌幅
                amplitude = tr.css('::text').extract()[5]  # 振幅
                close_open_price = tr.css('::text').extract()[6]  # 昨收/今开盘
                lowest_price = tr.css('::text').extract()[7]  # 最高/最低价
                volume = tr.css('::text').extract()[8]  # 成交量
                market_value = tr.css('::text').extract()[9]  # 市值
                ratio = tr.css('::text').extract()[10]  # 市盈率
                groups = tr.css('::text').extract()[11]  # 行业板块
                listing = tr.css('::text').extract()[12]  # 上市地
                stock_item = American_Stock_Item()
                stock_item['stock_code'] = stock_code
                stock_item['stock_name'] = stock_name
                stock_item['latest_price'] = latest_price
                stock_item['rise_fall'] = rise_fall
                stock_item['applies'] = applies
                stock_item['amplitude'] = amplitude
                stock_item['close_open_price'] = close_open_price
                stock_item['lowest_price'] = lowest_price
                stock_item['volume'] = volume
                stock_item['market_value'] = market_value
                stock_item['ratio'] = ratio
                stock_item['groups'] = groups
                stock_item['listing'] = listing
                yield stock_item

            # 点击下一页
            self.num += 1
            self.browser.find_element_by_xpath('//*[@id="pages"]/a[last()]').click()
            self.response = HtmlResponse(url=self.browser.current_url, body=self.browser.page_source,
                                         encoding="utf8")
            time.sleep(1)
