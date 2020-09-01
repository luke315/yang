# -*- coding: utf-8 -*-
import re
import time
import scrapy
from lib.lib_args import save_unit 
import datetime
from ..items import MyproItem

num_dict = {'num': 1, 'date_time': [], 'explan': 0}


class DailyfxSpider(scrapy.Spider):

    # 指定爬取日期
    # start_args = '2019-05-21'
    # start_time = datetime.date(*map(int, start_args.split('-')))
    # start_urls = ['https://www.dailyfxasia.com/calendar/getData/%s/%s' % (start_args, start_args)]

    name = 'dailyfx_before'
    # allowed_domains = ['https://www.dailyfxasia.com/calendar']

    # today之前
    start_t = datetime.timedelta(days=1)
    current_time = datetime.date.today()
    start_urls = ['https://www.dailyfxasia.com/calendar/getData/%s/%s' % (current_time - start_t, current_time - start_t)]

    custom_settings = {
        'ITEM_PIPELINES': {'dailyfx.pipelines.DailyfxPipeline1': 200},

        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': './././Logs/%s-%s.log' % (name, current_time)
    }


    def get_url(self):
        base_url = 'https://www.dailyfxasia.com/calendar/getData'
        timetel_t = datetime.timedelta(days=num_dict['num'])  # timedelta对象
        # 指定日期
        # new_time = self.start_time - timetel_t

        new_time = self.current_time - timetel_t - self.start_t
        num_dict['explan'] = 0
        # 拼接路由
        # time.sleep(3)

        # 爬到2006-01-01结束
        if str(new_time) == '2006-01-01':
            self.crawler.engine.close_spider(self)
        else:
            pass
        next_url = '%s/%s/%s' % (base_url, new_time, new_time)
        return next_url

    #  判断取得值是否为空
    def vavild(self, data):
        if len(data):
            return data[0]
        else:
            return ''

    def parse(self, response):
        tbody_list = response.css('.tab-pane')
        for tbody in tbody_list:
            real_date_data = re.findall(r'\d+', tbody.css('.dfx-eco-table-date ::text').extract_first())  # 日期
            real_date = f'{real_date_data[0]}/{real_date_data[1]}/{real_date_data[2]}'
            tr_list = tbody.css('.dfx-calendar-item')

            for tr in tr_list:
                real_time_data = tr.css('.hidden-xs ::text').re(r'\n(.*?)\n')  # 可为空
                real_time = self.vavild(real_time_data).strip()
                country_data = tr.css('.flag-20::text').re(r'\n(.*?)\n')
                country = self.vavild(country_data).strip()
                theme = tr.css('.dfx-event-title-item ::text')[-1].re(r'\n(.*?)\n')[0].strip()
                importance_data = tr.css('.label::text').re(r'\n(.*?)\n')
                importance = self.vavild(importance_data).strip()

                # 提取判断是否存在结果
                data = re.search('div', str(tr.css('.dfx-event-data-mobile').re(r'<(.*?)>')))
                if data is None:
                    result = ''
                    e_Forecast = ''
                    b_value = ''
                else:
                    result_date = tr.css('.dfx-event-data-mobile>div>span').re(r'>(.*?)<')  # 可为空
                    result = self.vavild(result_date).strip()
                    e_Forecast_date = tr.css('.dfx-event-data-mobile>div')[1].re(r':(.*?)<')  # 可为空
                    e_Forecast = self.vavild(e_Forecast_date).strip()
                    b_value_data = tr.css('.dfx-event-data-mobile>div')[2].re(r':(.*?)<')  # 可为空
                    b_value = self.vavild(b_value_data).strip()


                # 调用方法分单位
                result, result_unit = save_unit(result)
                e_Forecast, e_Forecast_unit = save_unit(e_Forecast)
                b_value, b_value_unit = save_unit(b_value)
                
                ex_data = re.search('div', str(tr.css('.hidden-xs')[-1].re(r'<(.*?)>')))
                if ex_data is None:
                    explan = ''
                else:
                    '''
                    <Selector xpath="descendant-or-self::*[@class and contains(concat(' ', normalize-space(@class), ' '), ' dfx-event-detail ')]/td/div/div" data='<div>\n                        德国研究机构─...'>
                    '''
                    res = str(response.css('.dfx-event-detail>td>div>div ')[num_dict['explan']].extract()).replace(
                        '</div>', '')
                    explan_data = re.search('br', str(
                        response.css('.dfx-event-detail>td>div>div')[num_dict['explan']].re(r'<(.*?)>')))
                    if explan_data is None:
                        explan = res.replace('<div>', '').strip()
                    else:
                        explan = res.replace('<div>', '').replace('<br>\r\n', '').strip()

                    num_dict['explan'] += 1
                # print(explan)

                dailyfx_item = MyproItem()

                # # 更新爬取时间
                # date_time = '%s%s' % (real_date, real_time)
                # num_dict['date_time'].append(date_time)

                # 把地址存入数据库，yield item对象
                dailyfx_item['real_date'] = real_date
                dailyfx_item['real_time'] = real_time
                dailyfx_item['country'] = country
                dailyfx_item['theme'] = theme
                dailyfx_item['importance'] = importance
                dailyfx_item['result'] = result
                dailyfx_item['result_unit'] = result_unit
                dailyfx_item['e_Forecast'] = e_Forecast
                dailyfx_item['e_Forecast_unit'] = e_Forecast_unit
                dailyfx_item['b_value'] = b_value
                dailyfx_item['b_value_unit'] = b_value_unit
                dailyfx_item['explan'] = explan
                # print(dailyfx_item)
                yield dailyfx_item

        next_url = self.get_url()
        num_dict['num'] += 1
        yield scrapy.Request(url=next_url, callback=self.parse, dont_filter=False)

