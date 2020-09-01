# -*- coding: utf-8 -*-
import scrapy
from lib.args.lib_args import current_time, spider_selenium_executable_path
from pro.items import American_Stock_Item
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


page_dict = {'page': 0, 'num': 1}

class AmericanStockSpider(scrapy.Spider):
    name = 'American_stock_info'
    start_urls = ["http://finance.sina.com.cn/stock/usstock/sector.shtml#cm"]
    update_time = ''

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
        self.browser = webdriver.Chrome(chrome_options=chrome_options,
                                        executable_path=spider_selenium_executable_path)
        super().__init__()


    def parse(self, response):
        if page_dict['num'] == 1:
            update_time = response.css('#updateTime ::text').extract_first()
            year = str(current_time).split('-')[0]
            month_day = update_time.split(' ')[0].replace('月', '-').split('日')[0]
            self.update_time = f'{year}-{month_day}'
            page_dict['num'] = 2
            self.browser.quit()
            url = self.get_next_url()
            yield scrapy.Request(url=url, callback=self.parse)
        else:
            tr_list = response.text.replace("/*<script>location.href='//sina.com';</script>*/", "").replace('null', '""')[2:-2]
            # print(eval(tr_list)['data'])
            if eval(tr_list)['data'] !=[]:
                for tr in eval(tr_list)['data']:
                    english_name = tr['name']  # 英文名称
                    stock_name = tr['cname']
                    category = tr['category']
                    category_id = tr['category_id']
                    stock_code = tr['symbol']
                    latest_price = tr['price']  # 最新价
                    rise_fall = tr['diff']  # 涨跌额
                    res = tr['chg']
                    if res == 'None':
                        res = '0'
                    applies = f"{res}%"  # 涨跌幅
                    amplitude = tr['amplitude']  # 振幅
                    close_price = tr['preclose']  # 昨收
                    open_price = tr['open']  # 今开
                    low_price = tr['low']  # 最低价
                    high_price = tr['high']  # 最高价
                    volume = tr['volume']  # 成交量
                    market_value = tr['mktcap']  # 市值
                    ratio = tr['pe']  # 市盈率
                    groups = tr['category']  # 行业板块
                    listing = tr['market']  # 上市地

                    if open_price == '0.00' and high_price == '0.00' and low_price == '0.00':
                        latest_price = '0.00'

                    stock_item = American_Stock_Item()
                    stock_item['stock_code'] = stock_code
                    stock_item['stock_name'] = stock_name.replace('\\', '')
                    stock_item['english_name'] = english_name
                    stock_item['latest_price'] = latest_price
                    stock_item['rise_fall'] = rise_fall
                    stock_item['applies'] = applies
                    stock_item['amplitude'] = amplitude
                    stock_item['close_price'] = close_price
                    stock_item['open_price'] = open_price
                    stock_item['low_price'] = low_price
                    stock_item['high_price'] = high_price
                    stock_item['volume'] = volume
                    stock_item['market_value'] = market_value
                    stock_item['ratio'] = ratio
                    stock_item['groups'] = groups
                    stock_item['listing'] = listing
                    stock_item['category'] = category
                    stock_item['category_id'] = category_id
                    stock_item['end_date'] = self.update_time
                    yield stock_item
                url = self.get_next_url()
                yield scrapy.Request(url=url, callback=self.parse)
            else:
                print('爬虫结束')

    def get_next_url(self):
        page_dict['page'] += 1
        url = f"http://stock.finance.sina.com.cn/usstock/api/jsonp.php//US_CategoryService.getList?page={page_dict['page']}&num=60"
        return url
