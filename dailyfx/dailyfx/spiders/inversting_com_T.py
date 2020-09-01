# -*- coding: utf-8 -*-
import copy
import datetime
import logging
import re
import time
from threading import Thread

import scrapy

from API.get_data_api import send_1999data, get_data_day_dingding
from dailyfx.items import Investing_Item
from lib.lib_args import save_unit, current_time, country_list

num_dict = {'num': 1, 'send_num': 0,  'date': ''}
Today_theme_data = {}
# 待去重的时间列表
list1 = []
# 监听的时间列表
date_time = []

# 熔断时间
# list_sec_time = [5, 10, 20, 30, 40, 50]
list_min_time = [2, 4, 6, 8, 10, 15, 25, 45, 60, 80, 120]
# 判断结果是否推送list
result_list = []
# 推送数据
data_all_list = []
# 去除不更新事件
delete_set = set([])

class InvestingComTSpider(scrapy.Spider):
    name = 'investing_com_T'

    custom_settings = {
        'ITEM_PIPELINES': {'dailyfx.pipelines.Investing_Item_Pipeline': 300},

        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': '././Logs/%s.%s.log' % (name, current_time)
    }

    header = {
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        'Content-Type': "application/x-www-form-urlencoded",
        'X-Requested-With': "XMLHttpRequest",
    }

    def start_requests(self):
        try:
            url = "https://cn.investing.com/economic-calendar/Service/getCalendarFilteredData"
            yield scrapy.FormRequest(url=url, headers=self.header, formdata={'currentTab': 'today'}, callback=self.parse)
        except Exception as e:
            logging.error(f'start_requests error :{e}')

    def parse(self, response):
        delete_set.clear()
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
                # 拼接事件
                y, m, d = date_time['date'].split('-')
                if len(m) == 1:
                    m = '0' + m
                if len(d) == 1:
                    d = '0' + d
                num_dict['date'] = f'{y}-{m}-{d}'
            else:
                country = td_list[1].select('span')[0].attrs['title'].encode('utf-8').decode('unicode_escape').replace(
                    '"', '')
                # 判断货币是否所需
                if country not in country_list:
                    continue
                real_date = date_time['date']
                real_time = td_list[0].get_text().encode('utf-8').decode('unicode_escape').replace(' ', '')
                res = re.findall('[\u4e00-\u9fa5]+', real_time)
                if res == []:
                    time_1 = '%s %s:%s' % (current_time, real_time, '00')
                    if real_time != '':
                        try:
                            self.time_update(time_1, 1)
                        except Exception as e:
                            print(e)
                    else:
                        pass
                else:
                    real_time = ''
                    time_1 = ''

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

                    res = Today_theme_data.get(theme_text)
                    if res != None:
                        theme = Today_theme_data[f'{theme_text}'].replace('Investing.com', '')
                    else:
                        try:
                            # 异步获取指标名
                            # theme = theme_get('https://cn.investing.com' + '/economic-calendar' + type_theme)
                            # 同步获取指标名
                            theme = self.theme_get('https://cn.investing.com' + '/economic-calendar' + type_theme).replace('Investing.com', '').replace(' ', '')
                            Today_theme_data[f'{theme_text}'] = theme
                        except Exception as e:
                            logging.error(f'获取指标名错误，指标：{theme_text}')
                            print(e)
                            continue

                # #  今日事件添加入集合待处理
                # delete_set.add(theme)

                # dingding api
                self.dingding_api(result, real_time, time_1, theme)

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

                dict_yide = dict(Investing_item)
                dict_yide.pop('type_theme')
                data_all_list.append(dict_yide)
                yield Investing_item


        # 易得api
        self.yide_api()

        # 循环
        next_url = self.get_url()
        yield scrapy.FormRequest(url=next_url, headers=self.header, formdata={'currentTab': 'today'}, callback=self.parse, dont_filter=True)

    def yide_api(self):
        # 0点推送给易得数据
        if num_dict['num'] == 1:
            send_list = copy.deepcopy(data_all_list)
            send_1999data(send_list)
        if num_dict['send_num'] == 1 and num_dict['num'] != 1:
            send_list = copy.deepcopy(data_all_list)
            send_1999data(send_list)
            num_dict['send_num'] = 0
        data_all_list.clear()

    def dingding_api(self, result, real_time, time_1, theme):
        if result != '':
            if real_time != '':
                # 有结果即去除监听事件
                try:
                    self.time_update(time_1, 2)
                except Exception as e:
                    print(e)
            # 第一次爬取不调用合作API
            if theme not in result_list:
                # 合作数据接口
                get_data_day_dingding(self.name, num_dict['date'], theme, result, real_time)
                num_dict['send_num'] = 1
                result_list.append(theme)

    def theme_get(self, url):
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

    # 翻页
    def get_url(self):
        # 过滤重复时间
        self.filter_list()
        # 去除过期时间
        self.time_x(num_dict['date'])
        # 拼接循环url
        num_dict['num'] += 1
        url = 'https://cn.investing.com/economic-calendar/Service/getCalendarFilteredData'
        while True:
            new_time = time.strftime("%Y-%m-%d %X")
            # 定时循环
            # print(type(new_time), new_time, type(date_time[0]), date_time[0],new_time in date_time )
            if new_time in date_time:
                date_time.remove(new_time)
                break
            # 解除当日循环
            elif date_time == [] or str(current_time) != num_dict['date']:
                # print(date_time)
                # print(type(str(current_time)), type(num_dict['date']))
                # print(str(current_time), num_dict['date'])
                # list1.clear()
                # date_time.clear()
                # num_dict['num'] = 1
                self.crawler.engine.close_spider(self)
                break
            else:
                time.sleep(0.5)
                continue
        return url

    # 更新时间
    def time_update(self, time_1, i):
        # 熔断时间
        if i == 1:
            list1.append(time_1)
            # 熔断时间
            # for i in list_sec_time:
            #     sec_Time = (datetime.datetime.strptime(time_1, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(
            #         seconds=i)).strftime("%Y-%m-%d %H:%M:%S")  # seconds=1
            #     list1.append(sec_Time)
            for i in list_min_time:
                min_Time = (datetime.datetime.strptime(time_1, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(
                    minutes=i)).strftime("%Y-%m-%d %H:%M:%S")  # minutes=1
                list1.append(min_Time)

        # 获取到结果后，删除时间片
        else:
            if time_1 in list1:
                list1.remove(time_1)
                # 去除以拿到结果时间
                # for i in list_sec_time:
                #     sec_Time = (datetime.datetime.strptime(time_1, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(
                #         seconds=i)).strftime("%Y-%m-%d %H:%M:%S")  # seconds=1
                #     if sec_Time in list1:
                #         list1.remove(sec_Time)
                for i in list_min_time:
                    min_Time = (datetime.datetime.strptime(time_1, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(
                        minutes=i)).strftime("%Y-%m-%d %H:%M:%S")  # minutes=1
                    if min_Time in list1:
                        list1.remove(min_Time)

    # 计算时间
    def time_x(self, real_date):
        from datetime import datetime
        # 日期格式话模版
        format_pattern = "%Y-%m-%d %H:%M:%S"
        # 具体日期 年/月/日 时/分/秒
        start_date = "%s 00:00:00" % real_date
        end_date = datetime.now()
        # 将 'datetime.datetime' 类型时间通过格式化模式转换为 'str' 时间
        end_date = end_date.strftime(format_pattern)
        # 将 'str' 时间通过格式化模式转化为 'datetime.datetime' 时间戳, 然后在进行比较
        # difference = (datetime.strptime(end_date, format_pattern) - datetime.strptime(start_date, format_pattern))
        # if difference.days < 1:
        #     pass
        # else:
        #     date_time.clear()
        # 去掉过期时间
        list_2 = []
        for i in date_time:
            list_2.append(i)
        for i in list_2:
            date_time_reduce = (
                    datetime.strptime(i, format_pattern) - datetime.strptime(end_date, format_pattern)).days
            if date_time_reduce < 0:
                date_time.remove(i)

    # 过滤重复时间
    def filter_list(self):
        # 时间去重
        for i in list1:
            if i not in date_time:
                date_time.append(i)

def async_func(f):
    def wrapper(*args, **kwargs):
        thr = Thread(target=f, args=args, kwargs=kwargs)
        thr.start()
    return wrapper

@async_func
def theme_get(url):
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