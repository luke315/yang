# -*- coding: utf-8 -*-
import re

import scrapy

from lib.args.lib_args import current_time
from lib.sql_setting import sql_set
from pro.items import Company_Profile_Item

dict_num = {'num': 1}

class WangyiCrawlSpider(scrapy.Spider):
    name = 's_z_stock_info'
    base_url = 'http://quotes.money.163.com'

    custom_settings = {
        # 'ITEM_PIPELINES': {'pro.pipelines.Sit_Psycopg2pipline': 300},
        # 'ITEM_PIPELINES': {'pro.pipelines.Pro_Psycopg2pipline': 300},
        'ITEM_PIPELINES': {'pro.pipelines.Psycopg2pipline': 100,
                           'pro.pipelines.Pro_Psycopg2pipline': 200,
                           'pro.pipelines.Sit_Psycopg2pipline': 300},

        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': '././Logs/%s.%s.log' % (name, current_time)
    }

    def start_requests(self):
        print('*' * 10 + '爬虫开始运行' + '*' * 10)
        tuple_stock = sql_set().read_sql()
        if tuple_stock is None:
            pass
        else:
            for i in tuple_stock:
                # print('正在爬取第%s' % dict_num['num'])
                stock_code_num = i[1].split('.')[0]
                # 传递股票代码和公司名字
                profit_item = Company_Profile_Item()
                profit_item['stock_code'] = i[1]
                profit_item['c_name'] = i[0]

                stock_url = '/f10/gszl_%s.html#01f02' % stock_code_num
                url = self.base_url + stock_url
                dict_num['num'] += 1
                yield scrapy.Request(url=url, callback=self.parse, dont_filter=False, meta={'items': profit_item})
        # 测试
        # url = 'http://quotes.money.163.com/f10/gszl_000032.html#01f02'
        # profit_item = Company_Profile_Item()
        # profit_item['stock_code'] = ''
        # profit_item['c_name'] = ''
        # yield scrapy.Request(url=url, callback=self.parse, dont_filter=False, meta={'items': profit_item})

    def is_null(self, td_list, num):
        result = re.findall('\S', td_list[num])
        if result == []:
            result = ''
        else:
            result = re.findall(r'>(.*?)<', td_list[num])[0]
        return result

    def is_null_2(self, td_list):
        if len(td_list) == 2:
            result = td_list[1].replace(' ', '').replace('\r\n', '')
        else:
            result = ''
        return result

    def parse(self, response):
        # 传值
        stock_code = response.meta['items']['stock_code']
        c_name = response.meta['items']['c_name']

        # 去表格标签
        table_list = response.css('.table_bg001')

        # 配置profit_item
        profit_table = table_list[0]
        profit_tr_list = profit_table.css('table>tr')
        res = profit_tr_list[0].css('td').extract()
        org = self.is_null(res, 1)
        address = self.is_null(res, 3)

        res = profit_tr_list[1].css('td').extract()
        chinese_f_short = self.is_null(res, 1)
        address_detail = self.is_null(res, 3)

        res = profit_tr_list[2].css('td').extract()
        company_name = self.is_null(res, 1)
        tel = self.is_null(res, 3)


        res = profit_tr_list[3].css('td').extract()
        english_name = self.is_null(res, 1)
        mail = self.is_null(res, 3)

        res = profit_tr_list[4].css('td').extract()
        r_capital = self.is_null(res, 1)
        chairman = self.is_null(res, 3)

        res = profit_tr_list[5].css('td').extract()
        staff_num = self.is_null(res, 1)
        secretary_name = self.is_null(res, 3)

        res = profit_tr_list[6].css('td').extract()
        legal_man = self.is_null(res, 1)
        secretary_tel = self.is_null(res, 3)

        res = profit_tr_list[7].css('td').extract()
        boss = self.is_null(res, 1)
        secretary_fax = self.is_null(res, 3)

        res = profit_tr_list[8].css('td').extract()
        c_inter = self.is_null(res, 1)
        secretary_mail = self.is_null(res, 3)

        main_business_res = profit_tr_list[10].xpath('td[2]/@title').extract_first()
        if main_business_res == None:
            main_business = profit_tr_list[10].css('td ::text').extract()[1].replace('\r\n', '').replace(' ', '').replace(r'\\', '')
        else:
            main_business = profit_tr_list[10].xpath('td[2]/@title').extract_first().replace('\r\n', '').replace(' ', '').replace(r'\\', '')

        business_scope_res = profit_tr_list[11].xpath('td[2]/@title').extract_first()
        if business_scope_res == None:
            business_scope = profit_tr_list[11].css('td ::text').extract()[1].replace('\r\n', '').replace(' ', '').replace(r'\\', '')
        else:
            business_scope = profit_tr_list[11].xpath('td[2]/@title').extract_first().replace('\r\n', '').replace(' ', '').replace(r'\\', '')

        c_history_res = profit_tr_list[12].xpath('td[2]/@title').extract_first()
        if c_history_res == None:
            c_history = profit_tr_list[12].css('td ::text').extract()[1].replace('\r\n', '').replace(' ', '').replace(r'\\', '')
        else:
            c_history = profit_tr_list[12].xpath('td[2]/@title').extract_first().replace('\r\n', '').replace(' ', '').replace(r'\\', '')

        profit_item = Company_Profile_Item()
        profit_item['stock_code'] = stock_code
        profit_item['c_name'] = c_name
        profit_item['org'] = org
        profit_item['address'] = address
        profit_item['chinese_f_short'] = chinese_f_short
        profit_item['address_detail'] = address_detail
        profit_item['company_name'] = company_name
        profit_item['tel'] = tel
        profit_item['english_name'] = english_name
        profit_item['mail'] = mail
        profit_item['r_capital'] = r_capital
        profit_item['chairman'] = chairman
        profit_item['staff_num'] = staff_num
        profit_item['secretary_name'] = secretary_name
        profit_item['legal_man'] = legal_man
        profit_item['secretary_tel'] = secretary_tel
        profit_item['boss'] = boss
        profit_item['secretary_fax'] = secretary_fax
        profit_item['c_inter'] = c_inter
        profit_item['secretary_mail'] = secretary_mail
        profit_item['main_business'] = main_business
        profit_item['business_scope'] = business_scope
        profit_item['c_history'] = c_history


        # 配置ipo_item
        ipo_table = table_list[1]
        ipo_tr_list = ipo_table.css('table>tr')
        res = ipo_tr_list[0].css('td ::text').extract()
        d_o_e = self.is_null_2(res)

        res = ipo_tr_list[1].css('td ::text').extract()
        m_d = self.is_null_2(res)

        # 增加两个字段
        profit_item['d_o_e'] = d_o_e
        profit_item['m_d'] = m_d
        yield profit_item

        # res = ipo_tr_list[2].css('td ::text').extract()
        # dis = self.is_null_2(res)
        # res = ipo_tr_list[3].css('td ::text').extract()
        # unit_price = self.is_null_2(res)
        # res = ipo_tr_list[4].css('td ::text').extract()
        # issue_num = self.is_null_2(res)
        # res = ipo_tr_list[5].css('td ::text').extract()
        # issue_price = self.is_null_2(res)
        # res = ipo_tr_list[6].css('td ::text').extract()
        # total_price = self.is_null_2(res)
        # res = ipo_tr_list[7].css('td ::text').extract()
        # issuance_fee = self.is_null_2(res)
        # res = ipo_tr_list[8].css('td ::text').extract()
        # issue_winning = self.is_null_2(res)
        # res = ipo_tr_list[9].css('td ::text').extract()
        # issue_pe = self.is_null_2(res)
        # res = ipo_tr_list[10].css('td ::text').extract()
        # per_share = self.is_null_2(res)
        # res = ipo_tr_list[11].css('td ::text').extract()
        # net_asset = self.is_null_2(res)
        # res = ipo_tr_list[12].css('td ::text').extract()
        # opening_pirce = self.is_null_2(res)
        # res = ipo_tr_list[13].css('td ::text').extract()
        # closing_price = self.is_null_2(res)
        # res = ipo_tr_list[14].css('td ::text').extract()
        # turnover_rate = self.is_null_2(res)
        # res = ipo_tr_list[15].css('td ::text').extract()
        # consignee = self.is_null_2(res)
        # res = ipo_tr_list[16].css('td ::text').extract()
        # referrer = self.is_null_2(res)
        # res = ipo_tr_list[17].css('td ::text').extract()
        # law_office = self.is_null_2(res)
        #
        # ipo_item = Company_IPO_Item()
        # ipo_item['stock_code'] = stock_code
        # ipo_item['c_name'] = c_name
        # ipo_item['d_o_e'] = d_o_e
        # ipo_item['m_d'] = m_d
        # ipo_item['dis'] = dis
        # ipo_item['unit_price'] = unit_price
        # ipo_item['issue_num'] = issue_num
        # ipo_item['issue_price'] = issue_price
        # ipo_item['total_price'] = total_price
        # ipo_item['issuance_fee'] = issuance_fee
        # ipo_item['issue_winning'] = issue_winning
        # ipo_item['issue_pe'] = issue_pe
        # ipo_item['per_share'] = per_share
        # ipo_item['net_asset'] = net_asset
        # ipo_item['opening_price'] = opening_pirce
        # ipo_item['closing_price'] = closing_price
        # ipo_item['turnover_rate'] = turnover_rate
        # ipo_item['consignee'] = consignee
        # ipo_item['referrer'] = referrer
        # ipo_item['law_office'] = law_office
        # yield ipo_item
        #
        # # 配置income_item
        # income_item = Company_Income_Item()
        # income_item['stock_code'] = stock_code
        # income_item['c_name'] = c_name
        #
        # income_product_table = table_list[3]
        # product_tr_list = income_product_table.css('table>tr')
        # str_list = re.findall('暂无数据', product_tr_list.css('::text').extract_first())
        # if str_list == []:
        #     for tr in product_tr_list:
        #         report_date = response.css('.report_date>span ::text').extract()[0]
        #         if '：' in report_date:
        #             report_date = report_date.split('：')[1].replace(' ', '')
        #         income_item['report_date'] = report_date
        #         classify = '产品'
        #         type_name = tr.css('td ::text').extract()[0]
        #         income = tr.css('td ::text').extract()[1]
        #         cost = tr.css('td ::text').extract()[2]
        #         profit = tr.css('td ::text').extract()[3]
        #         profit_rate = tr.css('td ::text').extract()[4]
        #         profits_account = tr.css('td ::text').extract()[5].replace(' ', '').replace('\r\n', '')
        #
        #         income_item['classify'] = classify
        #         income_item['type_name'] = type_name
        #         income_item['income'] = income
        #         income_item['cost'] = cost
        #         income_item['profit'] = profit
        #         income_item['profit_rate'] = profit_rate
        #         income_item['profits_account'] = profits_account
        #         yield income_item
        # else:
        #     pass
        #
        # income_industry_table = table_list[4]
        # industry_tr_list = income_industry_table.css('table>tr')
        # str_list = re.findall('暂无数据', industry_tr_list.css('::text').extract_first())
        # if str_list == []:
        #     for tr in industry_tr_list:
        #         report_date = response.css('.report_date>span ::text').extract()[1]
        #         if '：' in report_date:
        #             report_date = report_date.split('：')[1].replace(' ', '')
        #         income_item['report_date'] = report_date
        #         classify = '行业'
        #         type_name = tr.css('td ::text').extract()[0]
        #         income = tr.css('td ::text').extract()[1]
        #         cost = tr.css('td ::text').extract()[2]
        #         profit = tr.css('td ::text').extract()[3]
        #         profit_rate = tr.css('td ::text').extract()[4]
        #         profits_account = tr.css('td ::text').extract()[5].replace(' ', '').replace('\r\n', '')
        #
        #         income_item['classify'] = classify
        #         income_item['type_name'] = type_name
        #         income_item['income'] = income
        #         income_item['cost'] = cost
        #         income_item['profit'] = profit
        #         income_item['profit_rate'] = profit_rate
        #         income_item['profits_account'] = profits_account
        #
        #         yield income_item
        # else:
        #     pass
        #
        #
        # income_place_table = table_list[5]
        # place_tr_list = income_place_table.css('table>tr')
        # str_list = re.findall('暂无数据', place_tr_list.css('::text').extract_first())
        # if str_list == []:
        #     for tr in place_tr_list:
        #         report_date = response.css('.report_date>span ::text').extract()[2]
        #         if '：' in report_date:
        #             report_date = report_date.split('：')[1].replace(' ', '')
        #         income_item['report_date'] = report_date
        #         classify = '地区'
        #         type_name = tr.css('td ::text').extract()[0]
        #         income = tr.css('td ::text').extract()[1]
        #         cost = tr.css('td ::text').extract()[2]
        #         profit = tr.css('td ::text').extract()[3]
        #         profit_rate = tr.css('td ::text').extract()[4]
        #         profits_account = tr.css('td ::text').extract()[5].replace(' ', '').replace('\r\n', '')
        #
        #         income_item['classify'] = classify
        #         income_item['type_name'] = type_name
        #         income_item['income'] = income
        #         income_item['cost'] = cost
        #         income_item['profit'] = profit
        #         income_item['profit_rate'] = profit_rate
        #         income_item['profits_account'] = profits_account
        #
        #         yield income_item
        # else:
        #     pass