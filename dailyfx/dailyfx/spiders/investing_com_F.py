# -*- coding: utf-8 -*-
import copy
import datetime
import re

from threading import Thread
import scrapy
from dailyfx.items import Investing_Item
from lib.lib_args import save_unit, current_time, country_list

num_dict = {'num': 1, 'send_num': 0,  'date': ''}

page_num = {'num': 0}

headers = {
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        'Content-Type': "application/x-www-form-urlencoded",
        'X-Requested-With': "XMLHttpRequest",
    }

class InvestingComTSpider(scrapy.Spider):
    name = 'investing_com_F'
    custom_settings = {
        'ITEM_PIPELINES': {'dailyfx.pipelines.Investing_Item_Pipeline': 300},

        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': '././Logs/%s.%s.log' % (name, current_time)
    }

    def start_requests(self):
        start_t = datetime.timedelta(days=num_dict['num'])
        end_t = datetime.timedelta(days=num_dict['num'])

        data = {'dateFrom': str(current_time + start_t),
                'dateTo': str(current_time + end_t),
                'currentTab': 'custom',
                }

        url = "https://cn.investing.com/economic-calendar/Service/getCalendarFilteredData"
        yield scrapy.FormRequest(url=url, headers=headers, formdata=data, callback=self.parse)

    def parse(self, response):
        date_time = {'date': ''}
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.text, 'lxml')  # 具有容错功能

        tr_list = soup.select('tr')
        for tr in tr_list:
            td_list = tr.select('td')
            if len(td_list) == 1:
                date_time['date'] = \
                    td_list[0].get_text().encode('utf-8').decode('unicode_escape').replace('Â', '').split('\xa0\xa0')[
                        0].replace('年', '-').replace('月', '-').replace('日', '')
            else:
                country = td_list[1].select('span')[0].attrs['title'].encode('utf-8').decode('unicode_escape').replace(
                    '"', '')
                # 判断货币是否所需
                if country not in country_list:
                    continue
                real_date = date_time['date']
                real_time = td_list[0].get_text().encode('utf-8').decode('unicode_escape').replace(' ', '')
                res = re.findall('[\u4e00-\u9fa5]+', real_time)
                if res != []:
                    real_time = ''
                importance_list = td_list[2].select('i')
                if importance_list == []:
                    importance = ''
                    result = ''
                    e_Forecast = ''
                    b_value = ''
                    type_theme = ''
                    theme_time = ''
                    theme = td_list[3].get_text().encode('utf-8').decode('unicode_escape').replace(' ', '')
                else:
                    # 去除空字符
                    result = td_list[4].get_text().replace('\xa0', '').replace(' ', '')
                    result = self.is_null(result)
                    e_Forecast = td_list[5].get_text().replace('\xa0', '').replace(' ', '')
                    e_Forecast = self.is_null(e_Forecast)
                    b_value = td_list[6].get_text().replace('\xa0', '').replace(' ', '')
                    b_value = self.is_null(b_value)
                    if re.search('grayFullBullishIcon', importance_list[2].attrs['class'][0].split('"')[1]) != None:
                        importance = '高'
                    elif re.search('grayFullBullishIcon', importance_list[1].attrs['class'][0].split('"')[1]) != None:
                        importance = '中'
                    else:
                        importance = '低'

                    # # 获取指标唯一url
                    type_theme = td_list[3].select('a')[0].attrs['href'].split('\\')[-2]

                    # # 获指标发布时间
                    theme_text = td_list[3].select('a')[0].get_text().encode('utf-8').decode(
                        'unicode_escape').replace('"', '').replace(' ', '')

                    theme_time = self.check_theme_time(theme_text)

                    # # 判断指标是否为初值
                    epx_time = tr.select('smallGrayP ')
                    if epx_time != []:
                        theme_time = '初值'
                    try:
                        # 异步获取指标名
                        # theme = theme('https://cn.investing.com' + '/economic-calendar' + type_theme)
                        # 同步获取指标名
                        theme = self.theme('https://cn.investing.com' + '/economic-calendar' + type_theme).replace(' ', '')
                        theme = theme.replace('Investing.com', '')
                    except Exception as e:
                        print(theme_text)
                        print(e)
                        continue

                # 分离单位
                e_Forecast, e_Forecast_unit = save_unit(e_Forecast)
                b_value, b_value_unit = save_unit(b_value)
                result, result_unit = save_unit(result)

                Investing_item = Investing_Item()
                Investing_item['real_date'] = real_date
                Investing_item['real_time'] = real_time
                Investing_item['country'] = country
                Investing_item['theme'] = theme
                Investing_item['type_theme'] = type_theme.replace('/', '')
                Investing_item['theme_time'] = theme_time
                Investing_item['importance'] = importance
                Investing_item['result'] = result
                Investing_item['e_Forecast'] = e_Forecast
                Investing_item['b_value'] = b_value
                Investing_item['result_unit'] = result_unit
                Investing_item['b_value_unit'] = b_value_unit
                Investing_item['e_Forecast_unit'] = e_Forecast_unit
                # print(Investing_item)
                yield Investing_item
        num_dict['num'] += 1
        # 爬预测多少天
        if num_dict['num'] > 7:
            pass
        else:
            start_t = datetime.timedelta(days=num_dict['num'])
            end_t = datetime.timedelta(days=num_dict['num'])
            data = {'dateFrom': str(current_time + start_t),
                    'dateTo': str(current_time + end_t),
                    'currentTab': 'custom',
                    }
            url = "https://cn.investing.com/economic-calendar/Service/getCalendarFilteredData"
            yield scrapy.FormRequest(url=url, headers=headers, formdata=data, callback=self.parse, dont_filter=False)

    def is_null(self, data):
        if data == '\xa0':
            data = ''
        else:
            data = data
        return data

    def check_theme_time(self, theme):
        # 获取所有括号内容
        res_list = re.findall(r'[(](.*?)[)]', theme)
        if res_list != []:
            re_season = re.search('第(.*?)季度', res_list[-1])
            re_mouth = re.search('月', res_list[-1])
            re_mouth1 = re.search('月度', res_list[-1])
            if re_season != None:
                theme_time = res_list[-1]
            elif re_mouth != None and re_mouth1 == None:
                theme_time = res_list[-1]
            else:
                theme_time = ''
        else:
            theme_time = ''
        return theme_time

    def theme(self, url):
        from requests_html import HTMLSession
        session = HTMLSession()
        header = {
            'content-type': "application/x-www-form-urlencoded",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
            'x-requested-with': "XMLHttpRequest",
        }
        r = session.get(url=url, headers=header)
        theme = r.html.xpath('//*[@id="leftColumn"]/h1')[0].text
        return theme

def async_func(f):
    def wrapper(*args, **kwargs):
        thr = Thread(target=f, args=args, kwargs=kwargs)
        thr.start()
    return wrapper

@async_func
def theme(url):
    from requests_html import HTMLSession
    session = HTMLSession()
    header = {
        'content-type': "application/x-www-form-urlencoded",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        'x-requested-with': "XMLHttpRequest",
    }
    r = session.get(url=url, headers=header)
    theme = r.html.xpath('//*[@id="leftColumn"]/h1')[0].text
    return theme