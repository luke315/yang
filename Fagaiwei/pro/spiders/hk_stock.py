# -*- coding: utf-8 -*-
import time

import scrapy
from scrapy.http import HtmlResponse
from selenium.webdriver import ActionChains

from lib.args.lib_args import current_time, spider_selenium_executable_path
from pro.items import HongKong_Stock_Item
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

num_dict = {'num': 0}

li_dict = ['综合企业', '软件服务', '建筑', '其他金融', '银行', '电讯', '医疗保健服务', '酒店娱乐', '农业产品', '食物饮品',
           '家庭电器及用品', '工业制品', '金属', '煤炭', '半导体', '资讯科技器材', '地产', '保险', '公用事业', '支援服务',
           '传媒印刷', '零售', '保健护理用品', '纺织制衣', '汽车', '原材料', '采矿', '石油及天然气']


class HkStockSpider(scrapy.Spider):
    name = 'hk_stock'
    start_urls = ['http://vip.stock.finance.sina.com.cn/mkt/#qbgg_hk']
    response = ''

    custom_settings = {
        'ITEM_PIPELINES': {'pro.pipelines.HongKong_Stock_Pipline': 300},

        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': '././Logs/%s.%s.log' % (name, current_time)
    }

    def __init__(self):
        # 实例化一个浏览器对象(实例化一次)
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # 使用无头谷歌浏览器模式
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        self.browser = webdriver.Chrome(chrome_options=chrome_options, executable_path=spider_selenium_executable_path)
        super().__init__()

    # 必须在整个爬虫结束后，关闭浏览器
    def closed(self, spider):
        print('爬虫结束')
        self.browser.quit()

    def parse(self, response):
        parent_classify = '恒生行业'
        self.browser.find_element_by_link_text('香港股市').click()
        # classify = '工业制品'
        for classify in li_dict:
            article = self.browser.find_element_by_link_text(f'{parent_classify}')
            ActionChains(self.browser).move_to_element(article).perform()
            time.sleep(1)
            self.browser.find_element_by_link_text(f'{classify}').click()
            time.sleep(2)
            self.response = HtmlResponse(url=self.browser.current_url, body=self.browser.page_source, encoding="utf8")

            if classify == '家庭电器及用品':
                classify = '家庭电器用品'
            try:
                page_num = self.browser.find_element_by_xpath('//*[@id="tbl_wrap"]/div/a[last()-1]').text
                while True:
                    tr_list = self.response.xpath('//*[@id="tbl_wrap"]/table/tbody/tr')
                    for tr in tr_list:
                        if tr.css('::text') == []:
                            break
                        else:
                            stock_code = tr.css('::text').extract()[0]
                            stock_name = tr.css('::text').extract()[1]
                            latest_price = tr.css('::text').extract()[2].replace('\u3000', '')  # 最新价
                            rise_fall = tr.css('::text').extract()[3]  # 涨跌额
                            applies = tr.css('::text').extract()[4]  # 涨跌幅
                            close_price = tr.css('::text').extract()[5]
                            open_price = tr.css('::text').extract()[6]  # 昨收/今开盘
                            high_price = tr.css('::text').extract()[7]
                            low_price = tr.css('::text').extract()[8]  # 最低价
                            volume_num = tr.css('::text').extract()[9]  # 成交量
                            volume_price = tr.css('::text').extract()[10]

                            stock_item = HongKong_Stock_Item()
                            stock_item['stock_code'] = stock_code
                            stock_item['stock_name'] = stock_name
                            stock_item['latest_price'] = latest_price
                            stock_item['rise_fall'] = rise_fall
                            stock_item['applies'] = applies
                            stock_item['close_price'] = close_price
                            stock_item['open_price'] = open_price
                            stock_item['high_price'] = high_price
                            stock_item['low_price'] = low_price
                            stock_item['volume_num'] = volume_num
                            stock_item['volume_price'] = volume_price
                            stock_item['groups'] = classify
                            stock_item['parent_groups'] = parent_classify
                            yield stock_item

                    # 点击下一页
                    self.browser.find_element_by_xpath('//*[@id="tbl_wrap"]/div/a[last()]').click()
                    time.sleep(1)
                    self.response = HtmlResponse(url=self.browser.current_url, body=self.browser.page_source,
                                                 encoding="utf8")

                    num_dict['num'] += 1
                    if int(page_num) < num_dict['num']:
                        num_dict['num'] = 1
                        break
            except Exception as e:
                # print(e)
                tr_list = self.response.xpath('//*[@id="tbl_wrap"]/table/tbody/tr')
                for tr in tr_list:
                    if tr.css('::text') == []:
                        break
                    else:
                        stock_code = tr.css('::text').extract()[0]
                        stock_name = tr.css('::text').extract()[1]
                        latest_price = tr.css('::text').extract()[2].replace('\u3000', '')  # 最新价
                        rise_fall = tr.css('::text').extract()[3]  # 涨跌额
                        applies = tr.css('::text').extract()[4]  # 涨跌幅
                        close_price = tr.css('::text').extract()[5]
                        open_price = tr.css('::text').extract()[6]  # 昨收/今开盘
                        high_price = tr.css('::text').extract()[7]
                        low_price = tr.css('::text').extract()[8]  # 最低价
                        volume_num = tr.css('::text').extract()[9]  # 成交量
                        volume_price = tr.css('::text').extract()[10]

                        stock_item = HongKong_Stock_Item()
                        stock_item['stock_code'] = stock_code
                        stock_item['stock_name'] = stock_name
                        stock_item['latest_price'] = latest_price
                        stock_item['rise_fall'] = rise_fall
                        stock_item['applies'] = applies
                        stock_item['close_price'] = close_price
                        stock_item['open_price'] = open_price
                        stock_item['high_price'] = high_price
                        stock_item['low_price'] = low_price
                        stock_item['volume_num'] = volume_num
                        stock_item['volume_price'] = volume_price
                        stock_item['groups'] = classify
                        stock_item['parent_groups'] = parent_classify
                        yield stock_item
