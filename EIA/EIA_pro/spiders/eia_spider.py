# -*- coding: utf-8 -*-
import re
import scrapy
from EIA_pro.items import Eia_Epx_Item, Eia_Basic_Item
from lib.lib_args import current_time, mysql_setting
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

class EiaSpider(scrapy.Spider):
    name = 'eia_spider'
    start_urls = ['https://www.eia.gov/opendata/qb.php']
    # start_urls = ['https://www.eia.gov/opendata/qb.php?category=714755']
    num = 0
    base_url = 'https://www.eia.gov/opendata/qb.php?'

    custom_settings = {
        # 'ITEM_PIPELINES': {'EIA_pro.pipelines.mysqlTwistedpipline': 300},
        'ITEM_PIPELINES': {'EIA_pro.pipelines.Eia_Postgre_Pipeline': 300,
                           },
        # 'EIA_pro.pipelines.Eia_Postgre_sit_Pipeline': 200
        # 'EIA_pro.pipelines.Eia_Postgre_pro_Pipeline': 200

        # log
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': './././Logs/%s-%s.log' % (name, current_time)
    }

    def parse(self, response):
        # 获取节点
        li_list = response.css('.main_col>ul>li')
        url = response.request.url
        # print('当前url:%s' % url)

        # 判断，首页父级为空
        res = re.search('\?', url)
        if res == None:
            epx_parent_code = 'star_node_page'
        else:
            epx_parent_code = url.split('?')[1]
        for i in li_list:
            epx_name = i.css('a ::text').extract_first()
            epx_code_url = i.css('a ::attr(href)').extract_first()
            next_page_url = 'https://www.eia.gov/opendata/qb.php' + epx_code_url

            Eia_Epx = Eia_Epx_Item()
            Eia_Epx['epx_name'] = epx_name.strip(' ').replace('\xa0', '')
            Eia_Epx['epx_parent_code'] = epx_parent_code
            Eia_Epx['epx_code'] = epx_code_url.replace('?', '')

            # 根据sdid判断是否为根页
            sdid = re.search('sdid', next_page_url)
            if sdid is None:
                Eia_Epx['epx_frequency'] = ''
                Eia_Epx['epx_units'] = ''

                if self.re_(epx_name):
                    continue
                else:
                    yield Eia_Epx
                    yield scrapy.Request(url=next_page_url, callback=self.parse, dont_filter=False)
            else:
                epx_units = i.css('::text')[2].extract().replace('(', '').replace(')', '').replace('\r\n', '').replace('\t', '').replace(' ', '')
                res = next_page_url.split('.')[-1]
                # 处理频率
                if res == 'A':
                    frequency = 'YEAR'
                elif res == 'Q':
                    frequency = 'SEASON'
                elif res == 'M':
                    frequency = 'MONTH'
                elif res == 'W':
                    frequency = 'WEEK'
                elif res == 'H':
                    frequency = 'HOUR'
                elif res == 'HL':
                    frequency = 'HOUR'
                elif res == '4':
                    frequency = 'WEEK'
                else:
                    frequency = 'DATE'

                Eia_Epx['epx_frequency'] = frequency
                Eia_Epx['epx_units'] = epx_units
                if self.re_(epx_name):
                    pass
                else:
                    yield Eia_Epx

                    # 一起爬指标值
                    # yield scrapy.Request(url=next_page_url, callback=self.parse_detail, dont_filter=False)

    # 过滤的指标
    def re_(self, name):
        num = 0
        li_list = ['District of Columbia', 'Virgin Islands', 'AEO', 'Annual Energy Outlook', 'South Atlantic', 'East South Central', 'Middle Atlantic', 'Mountain', 'New England', 'Pacific Contiguous', 'Pacific Noncontiguous', 'East North Central', 'West North Central', 'West South Central', 'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming', 'Washington, D.C.', 'American Samoa', 'Guam', 'Northern Mariana Islands', 'The Commonwealth of Puerto Rico', 'U.S. Virgin Islands']
        for i in li_list:
            res = re.search(r'%s' % i, name)
            if res == None:
                pass
            else:
                num = 1
                break
        if num == 0:
            return False
        else:
            return True

    # 爬指标值
    def parse_detail(self, response):
        url = response.request.url
        epx_parent_code = url.split('?')[1]
        tr_list = response.css('.basic_table>tbody>tr')

        # 详情
        Eia_Basic = Eia_Basic_Item()
        for td in tr_list:
            epx_value = td.css('td ::text').extract()[3]
            period = td.css('td ::text').extract()[1]
            res = td.css('td ::text').extract()[2]
        #     series_name = td.css('td ::text').extract()[0]
        #     units = td.css('td ::text').extract()[4]
            if res == 'A':
                frequency = 'YEAR'
            elif res == 'Q':
                frequency = 'SEASON'
            elif res == 'M':
                frequency = 'MONTH'
            elif res == 'W':
                frequency = 'WEEK'
            elif res == 'H':
                frequency = 'HOUR'
            elif res == 'HL':
                frequency = 'HOUR'
            elif res == '4':
                frequency = 'WEEK'
            else:
                frequency = 'DATE'

            Eia_Basic['epx_code'] = epx_parent_code
            Eia_Basic['period'] = period
            Eia_Basic['epx_value'] = epx_value

            Eia_Basic['frequency'] = frequency
        #     Eia_Basic['series_name'] = series_name
        #     Eia_Basic['units'] = units

            # print('Eia_Basic:%s' % Eia_Basic)
            yield Eia_Basic

