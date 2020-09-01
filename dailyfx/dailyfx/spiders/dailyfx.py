# -*- coding: utf-8 -*-
import copy
import re
import time
import scrapy
import datetime
from lib.lib_args import save_unit
from API.get_data_api import get_data_day_dingding, send_1999data
from ..items import MyproItem

current_time = datetime.date.today()
num_dict = {'num': 1, 'send_num': 0,  'date': '', 'explan': 0}

# 待去重的时间列表
list1 = []
# 监听的时间列表
date_time = []

# 熔断时间
list_sec_time = [5, 10, 20, 30, 40, 50]
list_min_time = [2, 3, 4, 5, 6, 8, 10, 15, 25, 45, 60]
# 判断结果是否推送list
result_list = []
# 推送数据
data_all_list = []


class DailyfxSpider(scrapy.Spider):
    name = 'dailyfx'
    # allowed_domains = ['https://www.dailyfxasia.com/calendar']
    start_urls = ['https://www.dailyfxasia.com/calendar/getData/%s/%s' % (current_time, current_time)]

    # setting配置
    custom_settings = {
        'ITEM_PIPELINES': {'dailyfx.pipelines.DailyfxPipeline1': 100},

        # log
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': './././Logs/%s-%s.log' % (name, current_time)
    }

    #  判断取得值是否为空
    def vavild(self, data):
        if len(data):
            return data[0]
        else:
            return ''

    # 翻页
    def get_url(self):
        base_url = 'https://www.dailyfxasia.com/calendar/getData'
        num_dict['explan'] = 0
        while True:
            new_time = time.strftime("%Y-%m-%d %X")
            # 定时循环
            # print(type(new_time), new_time, type(date_time[0]), date_time[0],new_time in date_time )
            if new_time in date_time:
                date_time.remove(new_time)
                break
            # 解除当日循环
            elif date_time == [] or str(current_time) != num_dict['date']:
                # list1.clear()
                # date_time.clear()
                # num_dict['num'] = 1
                self.crawler.engine.close_spider(self)
                break
            else:
                time.sleep(0.5)
                continue

        # 拼接路由
        next_url = '%s/%s/%s' % (base_url, current_time, current_time)
        return next_url

    # 更新时间
    def time_update(self, time_1, i):
        # 熔断时间
        if i == 1:
            list1.append(time_1)
            # 熔断时间
            for i in list_sec_time:
                sec_Time = (datetime.datetime.strptime(time_1, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(
                    seconds=i)).strftime("%Y-%m-%d %H:%M:%S")  # seconds=1
                list1.append(sec_Time)
            for i in list_min_time:
                min_Time = (datetime.datetime.strptime(time_1, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(
                    minutes=i)).strftime("%Y-%m-%d %H:%M:%S")  # minutes=1
                list1.append(min_Time)

        # 获取到结果后，删除时间片
        else:
            if time_1 in list1:
                list1.remove(time_1)
                # 去除以拿到结果时间
                for i in list_sec_time:
                    sec_Time = (datetime.datetime.strptime(time_1, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(
                        seconds=i)).strftime("%Y-%m-%d %H:%M:%S")  # seconds=1
                    if sec_Time in list1:
                        list1.remove(sec_Time)
                    else:
                        pass
                for i in list_min_time:
                    min_Time = (datetime.datetime.strptime(time_1, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(
                        minutes=i)).strftime("%Y-%m-%d %H:%M:%S")  # minutes=1
                    if min_Time in list1:
                        list1.remove(min_Time)
                    else:
                        pass
            else:
                pass

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

    def parse(self, response):
        tbody_list = response.css('.tab-pane')
        for tbody in tbody_list:
            real_date_data = re.findall(r'\d+', tbody.css('.dfx-eco-table-date ::text').extract_first())  # 日期
            real_date = f'{real_date_data[0]}/{real_date_data[1]}/{real_date_data[2]}'
            num_dict['date'] = f'{real_date_data[0]}-{real_date_data[1]}-{real_date_data[2]}'
            tr_list = tbody.css('.dfx-calendar-item')

            for tr in tr_list:
                real_time_data = tr.css('.hidden-xs ::text').re(r'\n(.*?)\n')  # 可为空
                real_time = self.vavild(real_time_data).strip()

                # # 更新二次获取结果，待爬取时间
                if real_time != '':
                    time_1 = '%s %s:%s' % (current_time, real_time, '00')
                    self.time_update(time_1, 1)
                else:
                    time_1 = ''
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
                    if result != '' and real_time != '':
                        # 有结果即去除监听事件
                        self.time_update(time_1, 2)

                        # 第一次爬取不调用合作API
                        if num_dict['num'] != 1:
                            if theme + result not in result_list:
                                # 合作数据接口
                                get_data_day_dingding(self.name, num_dict['date'], theme, result)
                                num_dict['send_num'] = 1
                                result_list.append(theme + result)
                            else:
                                pass
                        else:
                            pass
                    else:
                        pass
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

                data_all_list.append(dict(dailyfx_item))
                yield dailyfx_item

        # 0点推送给易得数据
        if num_dict['num'] == 1 and num_dict['send_num'] == 0:
            send_list = copy.deepcopy(data_all_list)
            send_1999data(send_list)
        else:
            pass

        # 过滤重复时间
        self.filter_list()
        # 去除过期时间
        self.time_x(num_dict['date'])
        # 拼接循环url
        num_dict['num'] += 1
        next_url = self.get_url()

        if num_dict['send_num'] == 0:
            data_all_list.clear()
        else:
            send_list = copy.deepcopy(data_all_list)
            send_1999data(send_list)

            num_dict['send_num'] = 0
            data_all_list.clear()
        yield scrapy.Request(url=next_url, callback=self.parse, dont_filter=True)

