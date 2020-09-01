# -*- coding: utf-8 -*-

import os
import re

import scrapy

from lib.args.lib_args import stock_file_path, current_time
from lib.sql_setting import sql_set
from pro.items import File_Item

class SpiderSpider(scrapy.Spider):
    name = 's_z_stock_pdf_info'
    base_url = 'https://vip.stock.finance.sina.com.cn'
    ftype_list = ['ndbg', 'zgsmsyxs']


    custom_settings = {
        'ITEM_PIPELINES': {'pro.pipelines.MysqlPipeline': 300},

        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': '././Logs/%s.%s.log' % (name, current_time)
    }
    
    def start_requests(self):
        print('*' * 10 + '爬虫开始运行' + '*' * 10)
        tuple_stock = sql_set().read_sql()
        for i in tuple_stock:
            stock_code_num = i[1].split('.')[0]
            for ftype in self.ftype_list:
                spider_file_item = File_Item()
                spider_file_item['file_type'] = ftype
                stock_url = '/corp/go.php/vCB_AllBulletin/stockid/%s.phtml?ftype=%s' % (stock_code_num, ftype)
                url = self.base_url + stock_url
                yield scrapy.Request(url=url, callback=self.parse, dont_filter=False, meta={'items': spider_file_item})

    def parse(self, response):
        ftype = response.meta['items']['file_type']
        company_name = response.css('#stockName ::text').extract_first()
        company_name = self.change_name(company_name)
        stock_code = response.css('#stockName>span ::text').extract_first().replace('(', '').replace(')', '')
        a_list = response.css('.datelist>ul>a')
        if a_list != []:
            for i in a_list:
                file_name = i.css('a ::text').extract_first()
                file_name = self.change_name(file_name)
                detail_url = i.css('a ::attr(href)').extract_first()

                spider_file_item = File_Item()
                spider_file_item['company_name'] = company_name
                spider_file_item['stock_code'] = stock_code
                spider_file_item['file_name'] = file_name
                spider_file_item['file_type'] = ftype
                yield scrapy.Request(url=self.base_url + detail_url, callback=self.parse_detail, meta={'items': spider_file_item},)

    def parse_detail(self, response):
        res = response.meta['items']
        file_name = res['file_name']
        stock_code = res['stock_code']
        company_name = res['company_name']
        ftype = res['file_type']
        date_re = re.findall('\d+', file_name)
        if date_re != []:
            date = date_re[0]
        else:
            date = ''

        spider_file_item = File_Item()
        spider_file_item['file_name'] = file_name
        spider_file_item['company_name'] = company_name

        # 获取html文件
        html_file = r'%s/%s/HTML/%s.html' % (stock_file_path, company_name, file_name)
        # self.save_html(response.body, file_name, company_name)

        # # 获取pdf文件
        pdf_url = response.xpath('//*[@id="allbulletin"]/thead/tr/th/font/a/@href').extract_first()
        if pdf_url != None:
            pdf_file = r'%s/%s/PDF/%s.PDF' % (stock_file_path, company_name, file_name)
            # yield scrapy.Request(url=pdf_url, callback=self.save_pdf, meta={'items': spider_file_item},)
            
        else:
            pdf_file = ''

        # 路径入库
        spider_file_item['stock_code'] = stock_code
        spider_file_item['pdf_file'] = pdf_file
        spider_file_item['html_file'] = html_file
        spider_file_item['file_date'] = date
        spider_file_item['file_type'] = ftype
        yield spider_file_item

    def save_pdf(self, response):
        res = response.meta['items']
        file_name = res['file_name']
        company_name = res['company_name']

        folder_path = r"%s/%s/PDF/" % (stock_file_path, company_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        f = open(r'%s/%s/PDF/%s.PDF' % (stock_file_path, company_name, file_name), 'ab')  # 存储图片，多媒体文件需要参数b（二进制文件）
        f.write(response.body)  # 多媒体存储content
        f.close()

    def save_html(self, content, file_name, company_name):

        folder_path = r"%s/%s/HTML/" % (stock_file_path, company_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        f = open(r'%s/%s/HTML/%s.html' % (stock_file_path, company_name, file_name), 'ab')  # 存储图片，多媒体文件需要参数b（二进制文件）
        f.write(content)  # 多媒体存储content
        f.close()

    def change_name(self, name):
        if '*' in name:
            new_name = name.replace('*', '')
        else:
            new_name = name
        return new_name

