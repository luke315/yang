# -*- coding: utf-8 -*-
import scrapy
from lib.args.lib_args import current_time
from pro.items import American_Stock_Item

page_dict = {'page': 1}

class AmericanStockSpider(scrapy.Spider):
    name = 'US_ChinaStock_info'
    start_urls = ["http://money.finance.sina.com.cn/q/api/jsonp_v2.php//US_ChinaStockService.getData?page=1&num=60"]

    custom_settings = {
        'ITEM_PIPELINES': {'pro.pipelines.American_Stock_Pipline': 300},

        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': '././Logs/%s.%s.log' % (name, current_time)
    }

    def parse(self, response):
        tr_list = response.text.replace("/*<script>location.href='//sina.com';</script>*/", "").replace('null', '""')[2:-2]
        # print(eval(tr_list))
        if eval(tr_list) != '':
            for tr in eval(tr_list):
                stock_name = tr['name']
                category = tr['concept_sort']
                stock_code = tr['symbol']
                latest_price = tr['price']  # 最新价
                rise_fall = tr['diff']  # 涨跌额
                applies = f"{tr['chg']}%"  # 涨跌幅
                close_price = tr['preclose']  # 昨收
                open_price = tr['open']  # 今开
                low_price = tr['low']  # 最低价
                high_price = tr['high']  # 最高价
                volume = tr['volume']  # 成交量
                market_value = tr['mktcap']  # 市值
                ratio = tr['pe']  # 市盈率
                listing = tr['market']  # 上市地

                category = self.group(category)
                listing = self.listing(listing)

                stock_item = American_Stock_Item()
                stock_item['stock_code'] = stock_code
                stock_item['stock_name'] = stock_name
                stock_item['latest_price'] = latest_price
                stock_item['rise_fall'] = rise_fall
                stock_item['applies'] = applies
                stock_item['close_price'] = close_price
                stock_item['open_price'] = open_price
                stock_item['low_price'] = low_price
                stock_item['high_price'] = high_price
                stock_item['volume'] = volume
                stock_item['market_value'] = market_value
                stock_item['ratio'] = ratio
                stock_item['listing'] = listing
                stock_item['category'] = category
                yield stock_item

            url = self.get_next_url()
            yield scrapy.Request(url=url, callback=self.parse)
        else:
            print('爬虫结束')

    def get_next_url(self):
        page_dict['page'] += 1
        url = f"http://money.finance.sina.com.cn/q/api/jsonp_v2.php//US_ChinaStockService.getData?page={page_dict['page']}&num=60"
        return url

    def group(self, num):
        if num == '1' or num == 1:
            group = '能源'
        elif num == '2' or num == 2:
            group = '原材料'
        elif num == '0' or num == 0:
            group = '其他'
        elif num == '3' or num == 3:
            group = '金属与采矿'
        elif num == '4' or num == 4:
            group = '房地产与建筑'
        elif num == '5' or num == 5:
            group = '电力'
        elif num == '6' or num == 6:
            group = '机械制造'
        elif num == '7' or num == 7:
            group = '运输物流'
        elif num == '8' or num == 8:
            group = '工业综合'
        elif num == '9' or num == 9:
            group = '可选消费'
        elif num == '10' or num == 10:
            group = '必需消费'
        elif num == '11' or num == 11:
            group = '医药'
        elif num == '12' or num == 12:
            group = '健保'
        elif num == '13' or num == 13:
            group = '银行'
        elif num == '14' or num == 14:
            group = '保险'
        elif num == '15' or num == 15:
            group = '综合金融'
        elif num == '16' or num == 16:
            group = '电信'
        elif num == '17' or num == 17:
            group = '信息技术'
        elif num == '18' or num == 18:
            group = '公用事业'
        else:
            group = ''
        return group

    def listing(self, num):
        if num == 'O':
            listing = 'NASDAQ'
        elif num == 'N':
            listing = 'NYSE'
        elif num == 'A':
            listing = 'AMEX'
        elif num == 'T':
            listing = 'OTCBB'
        elif num == 'PK':
            listing = 'PINK'
        else:
            listing = ''
        return listing