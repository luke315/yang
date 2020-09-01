# -*- coding: utf-8 -*-
import json

import scrapy

from lib.args.lib_args import current_time
from pro.items import HongKong_Stock_Item

page_dict = {'page': 1}

class HkStockSpider(scrapy.Spider):
    name = 'hk_stock_info'
    start_urls = ['http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHKStockData?page=1&num=80&node=qbgg_hk']

    custom_settings = {
        'ITEM_PIPELINES': {'pro.pipelines.HongKong_Stock_Pipline': 300},
        #
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': '././Logs/%s.%s.log' % (name, current_time)
    }

    def parse(self, response):
        tr_list = json.loads(response.text)
        if tr_list != []:
            for tr in list(tr_list):
                try:
                    update_time = tr['ticktime'].split(' ')[0]
                    stock_code = tr['symbol']
                    stock_name = tr['name']
                    latest_price = tr['lasttrade']  # 最新价(今收)
                    rise_fall = tr['pricechange']  # 涨跌额
                    changepercent = tr['changepercent']
                    if changepercent == 'None':
                        changepercent = '0'
                    applies = f"{changepercent}%"  # 涨跌幅
                    close_price = tr['prevclose']  # 昨收
                    open_price = tr['open']  # 今开盘
                    high_price = tr['high']  # 最高
                    low_price = tr['low']  # 最低价
                    volume_num = tr['volume']  # 成交量
                    volume_price = tr['amount']  # 成交额

                    if open_price == '0.000' and high_price == '0.000' and low_price == '0.000':
                        latest_price = '0.000'
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
                    stock_item['end_date'] = update_time
                    yield stock_item
                except Exception as e:
                    print(e)

            url = self.get_next_url()
            yield scrapy.Request(url=url, callback=self.parse)
        else:
            print('爬虫结束')

    def get_next_url(self):
        page_dict['page'] += 1
        url = f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHKStockData?page={page_dict['page']}&num=80&node=qbgg_hk"
        return url
