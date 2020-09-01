# -*- coding: utf-8 -*-
import re

import psycopg2
import scrapy
from EIA_pro.items import Eia_Basic_Item
from lib.db_args import database_info_target2
from lib.lib_args import current_time, mysql_setting

next_url_list = []


class EiaAddSpider(scrapy.Spider):
    name = 'eia_add_api'
    frequency = ''
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

    def __init__(self, frequency=None, *args, **kwargs):
        super(EiaAddSpider, self).__init__(*args, **kwargs)
        self.frequency = frequency

    def start_requests(self):
        print(f'频率 {self.frequency} 爬虫开始')
        res = mysql_setting()
        api_key = '6fcfa943f07b0ed9027b7104471b8192'
        mysql_result = res.read_sql(self.frequency)
        num = 1
        for i in mysql_result:
            try:
                leaf_url = eval(i[0]).get('id')
                s_id = leaf_url.split('&')[1].split('=')[1]
                url = f'https://api.eia.gov/series/?api_key={api_key}&series_id={s_id}'
                # 去除AEO
                res = re.search(r'AEO', leaf_url)
                Eia_Basic = Eia_Basic_Item()
                Eia_Basic['epx_code'] = leaf_url
                if res != None:
                    pass
                else:
                    yield scrapy.Request(url=url, callback=self.parse, dont_filter=True, meta={'items': Eia_Basic})
                print(f'总url条：{len(mysql_result)}， 已爬取url：{num}条')
                num += 1
            except Exception as e:
                print(e)
        print(f'频率 {self.frequency} 爬虫结束')

        # leaf_url = 'category=235506&sdid=PET.WDIRPP42.4'
        # s_id = leaf_url.split('&')[1].split('=')[1]
        # url = f'https://api.eia.gov/series/?api_key={api_key}&series_id={s_id}'
        # Eia_Basic = Eia_Basic_Item()
        # Eia_Basic['epx_code'] = leaf_url
        # yield scrapy.Request(url=url, callback=self.parse, dont_filter=True, meta={'items': Eia_Basic})

    def parse(self, response):
        epx_parent_code = response.meta['items']['epx_code']
        json_obj = response.css('::text').extract_first().replace('null', '""')
        ser_obj = eval(json_obj)['series'][0]
        # name = ser_obj['name']
        # units = ser_obj['units']
        res = ser_obj['f']

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

        value_data = ser_obj['data']
        # 详情

        num = 1
        for i in value_data:
            if num < 7:
                period = i[0]
                epx_value = i[1]
                Eia_Basic = Eia_Basic_Item()
                Eia_Basic['frequency'] = frequency
                Eia_Basic['epx_code'] = epx_parent_code
                Eia_Basic['period'] = period
                Eia_Basic['epx_value'] = epx_value
                num += 1
                # print(Eia_Basic)
                yield Eia_Basic
            else:
                break

