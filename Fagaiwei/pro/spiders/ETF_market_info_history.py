# -*- coding: utf-8 -*-
import scrapy
from lib.args.lib_args import current_time, delete_unit
from lib.sql_setting import sql_set
from pro.items import ETF_Item


class ETFSpider(scrapy.Spider):
    name = 'ETF_market_info_history'

    start_time = '2020-01-01'
    end_time = f'{current_time}'
    type_fund = ['zyjl', 'jzzs']

    custom_settings = {
        'ITEM_PIPELINES': {'pro.pipelines.ETF_pipline': 300},

        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': '././Logs/%s.%s.log' % (name, current_time)
    }

    def start_requests(self):
        # 获取基金code和name
        fund_code_list = sql_set().read_fund_code()
        if fund_code_list != []:
            for code, fund_name, s_name in fund_code_list:
                etf_item = ETF_Item()
                etf_item['stock_code'] = code
                etf_item['stock_name'] = fund_name
                etf_item['fund_sname'] = s_name

                # # 净值和折溢价历史数据
                # # url = f'http://quotes.money.163.com/fund/{self.type_fund[0]}_{code}.html?start={self.start_time}&end={self.end_time}&sort=TDATE&order=desc'
                # url = f'http://quotes.money.163.com/fund/{self.type_fund[0]}_{code}.html'
                # yield scrapy.Request(url=url, callback=self.parse, dont_filter=False, meta={'items': etf_item})

                # # 投资组合历史数据
                url = f'http://quotes.money.163.com/fund/cgmx_{code}.html'
                yield scrapy.Request(url=url, callback=self.parse_zchy_cgmx, dont_filter=False, meta={'items': etf_item})


        # ---------------------------------------------- #
        # 基金测试代码（单只）
        # code = '510190'
        # etf_item = ETF_Item()
        # etf_item['stock_code'] = code
        # etf_item['stock_name'] = '华安上证龙头ETF'
        # # url = f'http://quotes.money.163.com/fund/{self.type_fund[0]}_{code}.html?start={self.start_time}&end={self.end_time}&sort=TDATE&order=desc'
        # # yield scrapy.Request(url=url, callback=self.parse, dont_filter=False, meta={'items': etf_item})
        #
        # url = f'http://quotes.money.163.com/fund/cgmx_{code}.html'
        # yield scrapy.Request(url=url, callback=self.parse_zchy_cgmx, dont_filter=False, meta={'items': etf_item})
        # ---------------------------------------------- #

    def parse(self, response):
        fund_code = response.meta['items']['stock_code']
        fund_name = response.meta['items']['stock_name']
        fund_sname = response.meta['items']['fund_sname']

        tr_list = response.css('.fn_cm_table>tbody>tr')
        etf_item = ETF_Item()
        etf_item['stock_code'] = fund_code
        etf_item['stock_name'] = fund_name
        etf_item['fund_sname'] = fund_sname
        for tr in tr_list:
            y_zyjl_time = tr.css('td ::text').extract()[0]
            fund_close = tr.css('td ::text').extract()[1].replace(',', '')
            fund_up_down_percent = tr.css('td ::text').extract()[2].replace(',', '')
            fund_volume_num = tr.css('td ::text').extract()[3].replace(',', '')
            fund_volume_price = tr.css('td ::text').extract()[4].replace(',', '')
            t_hsl = tr.css('td ::text').extract()[5].replace(',', '')
            t_zjl = tr.css('td ::text').extract()[6].replace(',', '')

            etf_item['fund_jzzs_time'] = ''
            etf_item['fund_zyjl_time'] = y_zyjl_time
            etf_item['fund_close'] = fund_close
            etf_item['fund_up_down_percent'] = fund_up_down_percent
            etf_item['fund_volume_num'] = delete_unit(fund_volume_num)
            etf_item['fund_volume_price'] = delete_unit(fund_volume_price)
            etf_item['fund_hsl'] = t_hsl
            etf_item['fund_zjl'] = t_zjl  # 不准确，次日更新
            etf_item['report_date'] = '1'
            yield etf_item

        if response.css('.pages_flip ::attr(href)').extract() != []:
            next_page = response.css('.pages_flip ::attr(href)').extract()[-1]
            # print(next_page)
            url = 'http://quotes.money.163.com' + next_page
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=False, meta={'items': etf_item})

        # url = f'http://quotes.money.163.com/fund/{self.type_fund[1]}_{fund_code}.html?start={self.start_time}&end={self.end_time}&sort=TDATE&order=desc'
        url = f'http://quotes.money.163.com/fund/{self.type_fund[1]}_{fund_code}.html'
        yield scrapy.Request(url=url, callback=self.parse_detail, dont_filter=False, meta={'items': etf_item})

    def parse_detail(self, response):
        fund_code = response.meta['items']['stock_code']
        fund_name = response.meta['items']['stock_name']
        fund_sname = response.meta['items']['fund_sname']

        tr_list = response.css('.fn_cm_table>tbody>tr')
        etf_item = ETF_Item()
        etf_item['stock_code'] = fund_code
        etf_item['stock_name'] = fund_name
        etf_item['fund_sname'] = fund_sname
        for tr in tr_list:
            y_jzzs_time = tr.css('td ::text').extract()[0]
            y_dwjz = tr.css('td ::text').extract()[1].replace(',', '')
            y_ljjz = tr.css('td ::text').extract()[2].replace(',', '')
            y_zzl = tr.css('td ::text').extract()[3].replace(',', '')

            etf_item['fund_zyjl_time'] = ''
            etf_item['fund_jzzs_time'] = y_jzzs_time
            etf_item['fund_dwjz'] = y_dwjz
            etf_item['fund_ljjz'] = y_ljjz
            etf_item['fund_zzl'] = y_zzl
            etf_item['report_date'] = '1'
            yield etf_item

        if response.css('.pages_flip ::attr(href)').extract() != []:
            next_page = response.css('.pages_flip ::attr(href)').extract()[-1]
            # print(next_page)
            url = 'http://quotes.money.163.com' + next_page
            yield scrapy.Request(url=url, callback=self.parse_detail, dont_filter=False, meta={'items': etf_item})

    def parse_zchy_cgmx(self, response):
        fund_code = response.meta['items']['stock_code']
        fund_name = response.meta['items']['stock_name']
        fund_sname = response.meta['items']['fund_sname']
        report_date_list = response.xpath('/html/body/div/div[7]/span/form/select/option/text()').extract()
        # print(fund_code, fund_name, report_date_list)
        etf_item = ETF_Item()
        etf_item['stock_code'] = fund_code
        etf_item['stock_name'] = fund_name
        etf_item['fund_sname'] = fund_sname
        if report_date_list != []:
            for report_date in report_date_list:
                etf_item['report_date'] = report_date
                url = f'http://quotes.money.163.com/fund/cgmx_{fund_code}.html?reportDate={report_date}'
                # print(url)
                yield scrapy.Request(
                    url=url,
                    callback=self.cgmx,
                    meta={'items': etf_item})

    # 持股明细
    def cgmx(self, response):
        fund_code = response.meta['items']['stock_code']
        fund_name = response.meta['items']['stock_name']
        fund_sname = response.meta['items']['fund_sname']
        report_date = response.url.split('=')[-1]

        etf_item = ETF_Item()
        etf_item['stock_code'] = fund_code
        etf_item['stock_name'] = fund_name
        etf_item['fund_sname'] = fund_sname
        etf_item['report_date'] = report_date
        list_data = response.css('#fn_fund_owner_01>table>tbody>tr')
        if list_data != []:
            for tr in list_data:
                if len(tr.css('td ::text').extract()) < 4:
                    break
                cgmx_code_name = tr.css('td ::text').extract()[0]
                cgmx_code = tr.css('td')[0].css('a ::attr(href)').extract_first()
                cgmx_code = cgmx_code.split('/')[-1].split('.')[0][1:7]
                cgmx_hold = tr.css('td ::text').extract()[1].replace(',', '')
                cgmx_value = tr.css('td ::text').extract()[2].replace(',', '')
                cgmx_account = tr.css('td ::text').extract()[3]

                etf_item['cgmx_code_name'] = cgmx_code_name
                etf_item['cgmx_code'] = cgmx_code
                etf_item['cgmx_account'] = cgmx_account
                etf_item['cgmx_hold'] = delete_unit(cgmx_hold)
                etf_item['cgmx_value'] = delete_unit(cgmx_value)
                etf_item['type_cgmx_or_zchy'] = 'cgmx'
                yield etf_item

        yield scrapy.Request(
            url=f'http://quotes.money.163.com/fund/zchy_{fund_code}.html?reportDate={report_date}',
            callback=self.zchy,
            meta={'items': etf_item})

    # 重仓行业
    def zchy(self, response):
        fund_code = response.meta['items']['stock_code']
        fund_name = response.meta['items']['stock_name']
        fund_sname = response.meta['items']['fund_sname']
        report_date = response.url.split('=')[-1]

        list_date = response.css('.fn_cm_table>tbody>tr')
        etf_item = ETF_Item()
        etf_item['stock_code'] = fund_code
        etf_item['stock_name'] = fund_name
        etf_item['fund_sname'] = fund_sname
        etf_item['report_date'] = report_date
        if list_date != []:
            for tr in list_date:
                if len(tr.css('td ::text').extract()) < 4:
                    break
                zchy_rank = tr.css('th ::text').extract_first()
                zchy_form = tr.css('td ::text').extract()[0]
                zchy_value = tr.css('td ::text').extract()[1].replace(',', '')
                zchy_account = tr.css('td ::text').extract()[2]
                zchy_account_change = tr.css('td ::text').extract()[3]

                etf_item['zchy_rank'] = zchy_rank
                etf_item['zchy_form'] = zchy_form
                etf_item['zchy_value'] = delete_unit(zchy_value)
                etf_item['zchy_account'] = zchy_account
                etf_item['zchy_account_change'] = zchy_account_change
                etf_item['type_cgmx_or_zchy'] = 'zchy'

                yield etf_item
