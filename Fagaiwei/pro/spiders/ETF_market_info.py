# -*- coding: utf-8 -*-
import logging
import re

import scrapy
import requests
from bs4 import BeautifulSoup
from lib.args.lib_args import current_time, delete_unit
from pro.items import ETF_Item


page_dict = {'page': 0}


class ETFSpider(scrapy.Spider):
    name = 'ETF_market_info'
    custom_settings = {
        'ITEM_PIPELINES': {'pro.pipelines.ETF_pipline': 300},

        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': '././Logs/%s.%s.log' % (name, current_time)
    }

    def start_requests(self):
        url = 'http://quotes.money.163.com/fn/service/fundtrade.php?&page=0&query=find:/ETF/;UPDOWN:_exists_true&count=100&fields=no,SYMBOL,NAME,SNAME,PRICE,UPDOWN,PERCENT,VOLUME,TURNOVER,OPEN,HIGH,LOW,YESTCLOSE,CODE&sort=PERCENT&order=desc'
        yield scrapy.Request(url=url, callback=self.parse, dont_filter=False)

    def parse(self, response):
        # print(response.text)
        data_obj = eval(response.text.replace('<!DOCTYPE HTML>', ''))
        # print(data_obj)
        data_list = data_obj['list']
        if data_list != []:
            # 获取时间
            # code = data_list[0]['SYMBOL']
            # if page_dict['page'] == 0:
            #     t_time, zfe, zzc = self.get_time(code)
            # 获取指标
            for i in data_list:
                try:
                    fund_code = i['SYMBOL']
                    fund_sname = i['NAME']
                    stock_name = i['SNAME']
                    fund_high = i['HIGH']  # 最高价
                    fund_low = i['LOW']  # 最低价
                    fund_open = i['OPEN']  # 今开
                    fund_close = i['PRICE']  # 最新价
                    fund_y_close = i['YESTCLOSE']  # 昨收
                    fund_up_down_percent = i['PERCENT']  # 涨跌幅
                    fund_up_down_percent = f"{'%.2f' % (float(fund_up_down_percent) * 100)}%"

                    fund_up_down = i['UPDOWN']  # 涨跌额
                    fund_volume_price = i['TURNOVER']  # 成交额
                    fund_volume_num = i['VOLUME']  # 成交量
                    # print(type(fund_volume_num), fund_volume_num)
                    # print(type(fund_volume_price), fund_volume_price)
                    if fund_volume_num == fund_volume_price == 0:
                        continue
                    else:
                        t_time = self.get_today_time()
                        zfe, zzc, y_zyjl_time, y_hsl, y_zjl, t_zyjl_time, t_hsl, t_zjl = self.get_time(fund_code)
                        y_jzzs_time, y_dwjz, y_ljjz, y_zzl = self.get_jz(fund_code)

                    etf_item = ETF_Item()
                    etf_item['fund_date'] = t_time
                    etf_item['fund_zfe'] = delete_unit(zfe)
                    etf_item['fund_zzc'] = delete_unit(zzc)
                    etf_item['fund_hsl'] = t_hsl
                    etf_item['fund_zjl'] = t_zjl  # 不准确，次日更新

                    # 更新昨日数据
                    etf_item['fund_y_hsl'] = y_hsl
                    etf_item['fund_y_zjl'] = y_zjl
                    etf_item['fund_dwjz'] = y_dwjz
                    etf_item['fund_ljjz'] = y_ljjz
                    etf_item['fund_zzl'] = y_zzl
                    etf_item['fund_jzzs_time'] = y_jzzs_time
                    etf_item['fund_zyjl_time'] = y_zyjl_time

                    etf_item['stock_code'] = fund_code
                    etf_item['stock_name'] = stock_name
                    etf_item['fund_sname'] = fund_sname
                    etf_item['fund_high'] = fund_high
                    etf_item['fund_low'] = fund_low
                    etf_item['fund_open'] = fund_open
                    etf_item['fund_close'] = fund_close
                    etf_item['fund_y_close'] = fund_y_close
                    etf_item['fund_up_down_percent'] = fund_up_down_percent
                    etf_item['fund_up_down'] = fund_up_down
                    etf_item['fund_volume_num'] = delete_unit(fund_volume_num)
                    etf_item['fund_volume_price'] = delete_unit(fund_volume_price)

                    etf_item['report_date'] = '1'
                    # print(etf_item)
                    yield etf_item

                    yield scrapy.Request(url=f'http://quotes.money.163.com/fund/cgmx_{fund_code}.html', callback=self.cgmx, meta={'items': etf_item})
                except Exception as e:
                    logging.error(e)

            url = self.get_next_url()
            yield scrapy.Request(url=url, callback=self.parse)
        else:
            print('爬虫结束')

    def get_next_url(self):
        page_dict['page'] += 1
        url = f"http://quotes.money.163.com/fn/service/fundtrade.php?&page={page_dict['page']}&query=find:/ETF/;UPDOWN:_exists_true&count=100&fields=no,SYMBOL,NAME,SNAME,PRICE,UPDOWN,PERCENT,VOLUME,TURNOVER,OPEN,HIGH,LOW,YESTCLOSE,CODE&sort=PERCENT&order=desc"
        return url

    def get_today_time(self):
        url = 'http://api.money.126.net/data/feed/0000001'
        r = requests.get(url)
        # soup = BeautifulSoup(r.text, 'lxml')  # 具有容错功能
        # res = soup.prettify()  # 处理好缩进，结构化显示
        # print(res)
        obj_data = eval(r.text.replace(');', '').replace('_ntes_quote_callback(', ''))
        t_time = obj_data['0000001']['time'].split(' ')[0].replace('/', '-')
        return t_time

    def get_time(self, code):
        num = 0
        while True:
            zyjl_url = f'http://quotes.money.163.com/fund/zyjl_{code}.html'
            zyjl_r = requests.get(zyjl_url)
            if zyjl_r.status_code == '500':
                num += 1
                continue
            elif num == 3:
                logging.error(f'{zyjl_url} 请求状态{zyjl_r.status_code}')
                return
            else:
                break
        zyjl_soup = BeautifulSoup(zyjl_r.text, 'lxml')  # 具有容错功能
        if zyjl_soup.select('.fn_cm_table>tbody>tr>td') != []:
            # t_time = zyjl_soup.select('.fn_cm_table>tbody>tr>td')[0].get_text()
            zfe = zyjl_soup.select('.fn_data_prop>li')[-1].get_text().split(':')[1].replace(',', '')
            zzc = zyjl_soup.select('.fn_data_prop>li')[-2].get_text().split(':')[1].replace(',', '')

            t_zyjl_time = zyjl_soup.select('.fn_cm_table>tbody>tr>td')[0].get_text()
            t_hsl = zyjl_soup.select('.fn_cm_table>tbody>tr>td')[5].get_text().replace(',', '')
            t_zjl = zyjl_soup.select('.fn_cm_table>tbody>tr>td')[6].get_text().replace(',', '')

            y_zyjl_time = zyjl_soup.select('.fn_cm_table>tbody>tr>td')[7].get_text()
            y_hsl = zyjl_soup.select('.fn_cm_table>tbody>tr>td')[12].get_text().replace(',', '')
            y_zjl = zyjl_soup.select('.fn_cm_table>tbody>tr>td')[13].get_text().replace(',', '')
            # res = soup.prettify()  # 处理好缩进，结构化显示
            # print(res)
            # print(r.text)
            # print(self.t_time)
            return zfe, zzc, y_zyjl_time, y_hsl, y_zjl, t_zyjl_time, t_hsl, t_zjl
        else:
            return '', '', '', '', '', '', '', ''

    def get_jz(self, code):
        num = 0
        while True:
            jzzs_url = f'http://quotes.money.163.com/fund/jzzs_{code}.html'
            jzzs_r = requests.get(jzzs_url)
            if jzzs_r.status_code == '500':
                num += 1
                continue
            elif num == 3:
                logging.error(f'{jzzs_url} 请求状态{jzzs_r.status_code}')
                return
            else:
                break
        jzzs_soup = BeautifulSoup(jzzs_r.text, 'lxml')  # 具有容错功能
        if jzzs_soup.select('.fn_cm_table>tbody>tr>td') != []:
            jzzs_time = jzzs_soup.select('.fn_cm_table>tbody>tr>td')[0].get_text()
            dwjz = jzzs_soup.select('.fn_cm_table>tbody>tr>td')[1].get_text().replace(',', '')
            ljjz = jzzs_soup.select('.fn_cm_table>tbody>tr>td')[2].get_text().replace(',', '')
            zzl = jzzs_soup.select('.fn_cm_table>tbody>tr>td')[3].get_text().replace(',', '')
            return jzzs_time, dwjz, ljjz, zzl
        else:
            return '', '', '', ''

    # 持股明细
    def cgmx(self, response):
        fund_code = response.meta['items']['stock_code']
        fund_name = response.meta['items']['stock_name']
        fund_sname = response.meta['items']['fund_sname']
        fund_date = response.meta['items']['fund_date']
        etf_item = ETF_Item()
        etf_item['stock_code'] = fund_code
        etf_item['stock_name'] = fund_name
        etf_item['fund_sname'] = fund_sname
        etf_item['fund_date'] = fund_date

        if response.xpath('/html/body/div/div[7]/span/form/select/option/text()').extract() != []:
            report_date = response.xpath('/html/body/div/div[7]/span/form/select/option/text()').extract()[0]
            etf_item['report_date'] = report_date

            list_data = response.css('#fn_fund_owner_01>table>tbody>tr')
            if list_data !=[]:
                for tr in list_data:
                    if len(tr.css('td ::text').extract()) < 4:
                        break
                    cgmx_code_name = tr.css('td ::text').extract()[0]
                    cgmx_code = tr.css('td')[0].css('a ::attr(href)').extract_first()
                    cgmx_code = cgmx_code.split('/')[-1].split('.')[0][1:7]
                    cgmx_hold = tr.css('td ::text').extract()[1].replace(',', '')
                    cgmx_value = tr.css('td ::text').extract()[2].replace(',', '')
                    cgmx_account = tr.css('td ::text').extract()[3].replace(',', '')

                    etf_item['cgmx_code_name'] = cgmx_code_name
                    etf_item['cgmx_code'] = cgmx_code
                    etf_item['cgmx_account'] = cgmx_account
                    etf_item['cgmx_hold'] = delete_unit(cgmx_hold)
                    etf_item['cgmx_value'] = delete_unit(cgmx_value)
                    etf_item['type_cgmx_or_zchy'] = 'cgmx'
                    yield etf_item

            yield scrapy.Request(url=f'http://quotes.money.163.com/fund/zchy_{fund_code}.html?reportDate={report_date}', callback=self.zchy, meta={'items': etf_item})

    # 重仓资产
    def zchy(self, response):
        fund_code = response.meta['items']['stock_code']
        fund_name = response.meta['items']['stock_name']
        fund_sname = response.meta['items']['fund_sname']
        report_date = response.url.split('=')[-1]
        fund_date = response.meta['items']['fund_date']

        list_date = response.css('.fn_cm_table>tbody>tr')
        etf_item = ETF_Item()
        etf_item['stock_code'] = fund_code
        etf_item['stock_name'] = fund_name
        etf_item['fund_sname'] = fund_sname
        etf_item['fund_date'] = fund_date
        etf_item['report_date'] = report_date
        if list_date != []:
            for tr in list_date:
                if len(tr.css('td ::text').extract()) < 4:
                    break
                zchy_rank = tr.css('th ::text').extract_first()
                zchy_form = tr.css('td ::text').extract()[0]
                zchy_value = tr.css('td ::text').extract()[1].replace(',', '')
                zchy_account = tr.css('td ::text').extract()[2].replace(',', '')
                zchy_account_change = tr.css('td ::text').extract()[3].replace(',', '')

                etf_item['zchy_rank'] = zchy_rank
                etf_item['zchy_form'] = zchy_form
                etf_item['zchy_value'] = delete_unit(zchy_value)
                etf_item['zchy_account'] = zchy_account
                etf_item['zchy_account_change'] = zchy_account_change
                etf_item['type_cgmx_or_zchy'] = 'zchy'

                yield etf_item


