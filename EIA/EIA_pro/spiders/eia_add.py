# -*- coding: utf-8 -*-
import re
import scrapy
from EIA_pro.items import Eia_Basic_Item
from lib.lib_args import current_time, mysql_setting

next_url_list = []

class EiaAddSpider(scrapy.Spider):
    name = 'eia_add'
    base_url = 'https://www.eia.gov/opendata/qb.php?'
    frequency = ''
    num = 0

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
        res = mysql_setting()
        mysql_result = res.read_sql(self.frequency)
        print(len(mysql_result))
        for i in mysql_result:
            leaf_url = eval(i[0]).get('id')
            # 去除AEO
            res = re.search(r'AEO', leaf_url)
            if res != None:
                pass
            else:
                url = self.base_url + leaf_url
                yield scrapy.Request(url=url, callback=self.parse, dont_filter=False)
        print('爬虫结束')


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

