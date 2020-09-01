# -*- coding: utf-8 -*-

import scrapy
from EIA_pro.items import Eia_Basic_Item
from scrapy_redis.spiders import RedisSpider
from lib.lib_args import current_time
next_url_list = []


class EiaAddSpider(RedisSpider):
    name = 'eia_redis'
    redis_key = "eia_redis:start_urls"

    custom_settings = {
        # 'ITEM_PIPELINES': {'EIA_pro.pipelines.mysqlTwistedpipline': 300},
        'ITEM_PIPELINES': {'EIA_pro.pipelines.Eia_Postgre_Pipeline': 300,
                           },
        # 'EIA_pro.pipelines.Eia_Postgre_sit_Pipeline': 200
        # 'EIA_pro.pipelines.Eia_Postgre_pro_Pipeline': 200
        # log
        # 'LOG_LEVEL': 'DEBUG',
        # 'LOG_FILE': './././Logs/%s-%s.log' % (name, current_time)
    }

    def parse(self, response):
        url = response.request.url
        epx_parent_code = url.split('?')[1]

        tr_list = response.css('.basic_table>tbody>tr')
        # 详情
        for td in tr_list:
            epx_value = td.css('td ::text').extract()[3]
            period = td.css('td ::text').extract()[1]
            res = td.css('td ::text').extract()[2]
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
            Eia_Basic = Eia_Basic_Item()
            Eia_Basic['frequency'] = frequency
            Eia_Basic['epx_code'] = epx_parent_code
            Eia_Basic['period'] = period
            Eia_Basic['epx_value'] = epx_value
            yield Eia_Basic

