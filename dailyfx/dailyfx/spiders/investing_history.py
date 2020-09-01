# -*- coding: utf-8 -*-
import copy
import re
import time
import scrapy
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from dailyfx.items import Investing_Item
from lib.lib_args import url_list, save_unit
from lib.rename import excel_save


class InvestingSpider(scrapy.Spider):
    name = 'investing_com'

    custom_settings = {
        'ITEM_PIPELINES': {'dailyfx.pipelines.Investing_Item_Pipeline': 300},

        # 'LOG_LEVEL': 'DEBUG',
        # 'LOG_FILE': '././Logs/%s-%s.log' % (name, current_time)
    }

    def __init__(self):
        # 实例化一个浏览器对象(实例化一次)
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # 使用无头谷歌浏览器模式
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        self.browser = webdriver.Chrome(chrome_options=chrome_options,
                                        executable_path='C:\Virtualenvs\EIA\Lib\site-packages\selenium\webdriver\chrome\chromedriver.exe')
        super().__init__()

    # 必须在整个爬虫结束后，关闭浏览器
    def closed(self, spider):
        print('爬虫结束')
        self.browser.quit()

    def start_requests(self):
        # exp_list = ['USD']
        exp_list = ['欧元区', '德国', '法国', '意大利', 'AUD', 'CAD', 'CNY', 'JPY', 'GBP', 'NZD', 'CHF', 'HKD', 'USD']
        for i in exp_list:
            url = f'https://cn.investing.com/search/?q={i}&tab=ec_event'
            url_list.append(url)
            Investing_item = Investing_Item()
            country = self.check_country(i)
            Investing_item['country'] = country
            yield scrapy.Request(url=url, callback=self.parse, meta={'items': Investing_item})

        # 测试代码----------------------------
        # Investing_item = Investing_Item()
        # country = self.check_country('USD')
        # Investing_item['country'] = country
        # url = 'https://cn.investing.com/search/?q=%E6%97%A5%E6%9C%AC%E6%9C%8D%E5%8A%A1%E4%B8%9A%E9%87%87%E8%B4%AD%E7%BB%8F%E7%90%86%E4%BA%BA%E6%8C%87%E6%95%B0(PMI)&tab=ec_event'
        # yield scrapy.Request(url=url, callback=self.parse, meta={'items': Investing_item})

    def parse(self, response):
        country = response.meta['items']['country']
        div_e = response.css('.economicEvents')[1]
        a_list = div_e.css('a')
        for a in a_list:
            base_url = 'https://cn.investing.com'
            next_url = a.css('::attr(href)').extract_first()
            # country = a.css('.second ::text').extract_first()
            theme = a.css('.fourth ::text').extract_first().replace(' ', '')
            importance_list = a.css('.third>label>i ::attr(class)').extract()
            if importance_list[2] == 'grayFullBullishIcon':
                importance = '高'
            elif importance_list[1] == 'grayFullBullishIcon':
                importance = '中'
            else:
                importance = '低'
            # print(country, importance)
            url = base_url + next_url
            url_num = next_url.split('-')[-1]

            Investing_item = Investing_Item()
            Investing_item['country'] = country
            Investing_item['theme'] = theme
            Investing_item['importance'] = importance
            Investing_item['url_num'] = url_num
            yield scrapy.Request(url=url, callback=self.parse_detail, meta={'items': Investing_item})

    def parse_detail(self, response):
        url_num = response.meta['items']['url_num']
        country = response.meta['items']['country']
        theme = response.meta['items']['theme']
        importance = response.meta['items']['importance']

        # 模拟点击接口，暂时不用
        # response = self.sele_click(url_num)

        # 下页请求
        event_id = ''
        event_attr_id = ''
        event_timestamp = ''
        type_theme = response.request.url.split('/')[-1]

        Investing_item = Investing_Item()
        Investing_item['url_num'] = url_num
        Investing_item['country'] = country
        Investing_item['theme'] = theme
        Investing_item['importance'] = importance
        Investing_item['type_theme'] = type_theme
        # 循环标签
        tr_list = response.css(f'#eventHistoryTable{url_num}>tbody>tr')
        for tr in tr_list:
            event_id = tr.css('::attr(id)').extract_first().split('_')[1]
            event_attr_id = tr.css('::attr(event_attr_id)').extract_first()
            event_timestamp = tr.css('::attr(event_timestamp)').extract_first()

            len_td = len(tr.css('td ::text'))
            if len_td == 2:
                real_date = tr.css('td ::text').extract()[0].replace('年', '-').replace('月', '-').replace('日', '')
                theme_time = ''
                real_time = tr.css('td ::text').extract()[1].replace('\xa0', '')
                result = ''
                e_Forecast = ''
                b_value = ''
            else:
                real_date = tr.css('td ::text').extract()[0].split(' ')[0].replace('年', '-').replace('月', '-').replace('日', '')
                theme_time = tr.css('td ::text').extract()[0].split(' ')[-1]
                real_time = tr.css('td ::text').extract()[1].replace('\xa0', '')
                epx_time = tr.css('.smallGrayP ').extract_first()
                if epx_time != None:
                    theme_time = '初值'
                result = tr.css('td ::text').extract()[2]
                result = self.is_null(result)
                e_Forecast = tr.css('td ::text').extract()[3]
                e_Forecast = self.is_null(e_Forecast)
                b_value = tr.css('td ::text').extract()[4]
                b_value = self.is_null(b_value)

                # print(result, e_Forecast, b_value)
            e_Forecast,  e_Forecast_unit = save_unit(e_Forecast)
            b_value, b_value_unit = save_unit(b_value)
            result, result_unit = save_unit(result)

            Investing_item['real_date'] = real_date
            Investing_item['theme_time'] = theme_time
            Investing_item['real_time'] = real_time
            Investing_item['result'] = result
            Investing_item['e_Forecast'] = e_Forecast
            Investing_item['b_value'] = b_value
            Investing_item['e_Forecast_unit'] = e_Forecast_unit
            Investing_item['b_value_unit'] = b_value_unit
            Investing_item['result_unit'] = result_unit

            # 超过T时间，不存储
            date_timestamp = int(time.mktime(time.strptime(real_date + ' ' + '00:00:00', "%Y-%m-%d %H:%M:%S")))
            now_timestamp = int(time.time())

            if now_timestamp > date_timestamp:
                yield Investing_item
            else:
                pass
        # print(event_id, event_attr_id, event_timestamp)

        # 超日期时间异常捕获
        while True:
            postUrl = "https://cn.investing.com/economic-calendar/more-history"
            payloadHeader = {
                'content-type': "application/x-www-form-urlencoded",
                'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
                'x-requested-with': "XMLHttpRequest",
            }
            payloadData = f'eventID={event_id}&event_attr_ID={event_attr_id}&event_timestamp={event_timestamp}&is_speech=0'
            from requests_html import HTMLSession
            session = HTMLSession()

            r = session.post(postUrl, headers=payloadHeader, data=payloadData)
            tr_list = r.html.find('tr')
            td = tr_list[0].find('td')
            res = td[0].text.replace(r'/td', '').replace(r'/tr', '').replace(r'\n', '').replace(r'<\>', '')\
                .replace(r'","hasMoreHistory":"1"}', '').encode('utf-8').decode('unicode_escape')
            list_data = res.split('<\/i>')
            num = 1
            list_data.pop(-1)

            for i in list_data:
                if num == 1:
                    date_time, real_time, result, e_Forecast, b_value, kong3 = i.split('\n')
                    num += 1
                else:
                    kong1, kong2, date_time, real_time, result, e_Forecast, b_value, kong3 = i.split('\n')
                real_date, theme_time = date_time.split(' ')
                real_date = real_date.replace('年', '-').replace('月', '-').replace('日', '')

                info = re.search(r'span', real_time)
                if info != None:
                    theme_time = '初值'
                real_time = real_time.replace(r'<\/span>', '')
                result = result.replace(r'<\/span>', '')

                e_Forecast, e_Forecast_unit = save_unit(e_Forecast)
                b_value, b_value_unit = save_unit(b_value)
                result, result_unit = save_unit(result)

                Investing_item = Investing_Item()
                Investing_item['country'] = country
                Investing_item['theme'] = theme
                Investing_item['importance'] = importance
                Investing_item['real_date'] = real_date
                Investing_item['theme_time'] = theme_time
                Investing_item['real_time'] = real_time.replace('Â\xa0', '')
                Investing_item['result'] = result
                Investing_item['e_Forecast'] = e_Forecast
                Investing_item['b_value'] = b_value
                Investing_item['e_Forecast_unit'] = e_Forecast_unit
                Investing_item['b_value_unit'] = b_value_unit
                Investing_item['result_unit'] = result_unit

                # 超过T时间，不存储
                date_timestamp = int(time.mktime(time.strptime(real_date + ' ' + '00:00:00', "%Y-%m-%d %H:%M:%S")))
                now_timestamp = int(time.time())
                if now_timestamp > date_timestamp:
                    yield Investing_item
                else:
                    pass

            event = tr_list[-1].attrs
            event_id = event.get('id').split('_')[1].replace(r'\"', '')
            event_attr_id = event.get('event_attr_id').replace(r'\"', '')
            event_timestamp = list(event.values())[-2].replace(r'\"', '') + ' ' + list(event.keys())[-1].replace(r'\"','')

            if len(tr_list) != 6:
                break

            a = "2017-01-01 00:00:00"
            # 转换为时间戳:
            b_time = int(time.mktime(time.strptime(event_timestamp, "%Y-%m-%d %H:%M:%S")))
            a_time = int(time.mktime(time.strptime(a, "%Y-%m-%d %H:%M:%S")))
            # print(event_id, event_attr_id, event_timestamp)
            if b_time < a_time:
                break

    def request_html_api(self, country, importance, theme, event_id, event_attr_id, event_timestamp):
        print('request_html')
        while True:
            postUrl = "https://cn.investing.com/economic-calendar/more-history"
            payloadHeader = {
                'content-type': "application/x-www-form-urlencoded",
                'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
                'x-requested-with': "XMLHttpRequest",
            }
            payloadData = f'eventID={event_id}&event_attr_ID={event_attr_id}&event_timestamp={event_timestamp}&is_speech=0'
            from requests_html import HTMLSession
            session = HTMLSession()

            r = session.post(postUrl, headers=payloadHeader, data=payloadData)
            tr_list = r.html.find('tr')
            td = tr_list[0].find('td')
            res = td[0].text.replace(r'/td', '').replace(r'/tr', '').replace(r'\n', '').replace(r'<\>', '').replace(
                r'<\/span>', '') \
                .replace(r'","hasMoreHistory":"1"}', '').encode('utf-8').decode('unicode_escape')
            list_data = res.split('<\/i>')
            num = 1
            list_data.pop(-1)
            for i in list_data:
                if num == 1:
                    date_time, real_time, result, e_Forecast, b_value, kong3 = i.split('\n')
                    num += 1
                else:
                    kong1, kong2, date_time, real_time, result, e_Forecast, b_value, kong3 = i.split('\n')
                real_date, theme_time = date_time.split(' ')

                e_Forecast, e_Forecast_unit = save_unit(e_Forecast)
                b_value, b_value_unit = save_unit(b_value)
                result, result_unit = save_unit(result)

                Investing_item = Investing_Item()
                Investing_item['country'] = country
                Investing_item['theme'] = theme
                Investing_item['importance'] = importance
                Investing_item['real_date'] = real_date
                Investing_item['theme_time'] = theme_time
                Investing_item['real_time'] = real_time
                Investing_item['result'] = result
                Investing_item['e_Forecast'] = e_Forecast
                Investing_item['b_value'] = b_value
                Investing_item['e_Forecast_unit'] = e_Forecast_unit
                Investing_item['b_value_unit'] = b_value_unit
                Investing_item['result_unit'] = result_unit
                yield Investing_item

            event = tr_list[5].attrs
            event_id = event.get('id').split('_')[1].replace(r'\"', '')
            event_attr_id = event.get('event_attr_id').replace(r'\"', '')
            event_timestamp = list(event.values())[-2].replace(r'\"', '') + ' ' + list(event.keys())[-1].replace(r'\"',                                                                                                '')

            a = "2017-01-01 00:00:00"
            # 转换为时间戳:
            b_time = int(time.mktime(time.strptime(event_timestamp, "%Y-%m-%d %H:%M:%S")))
            a_time = int(time.mktime(time.strptime(a, "%Y-%m-%d %H:%M:%S")))
            if b_time < a_time:
                break

    # 重写requests
    def requests_payload(self, event_id, event_attr_id, event_timestamp):
        # 爬到
        a = "2015-01-01 00:00:00"
        # 转换为时间戳:
        b_time = int(time.mktime(time.strptime(event_timestamp, "%Y-%m-%d %H:%M:%S")))
        a_time = int(time.mktime(time.strptime(a, "%Y-%m-%d %H:%M:%S")))
        if b_time > a_time:
            postUrl = "https://cn.investing.com/economic-calendar/more-history"
            payloadHeader = {
                'content-type': "application/x-www-form-urlencoded",
                'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
                'x-requested-with': "XMLHttpRequest",
            }
            payloadData = f'eventID={event_id}&event_attr_ID={event_attr_id}&event_timestamp={event_timestamp}&is_speech=0'
            yield scrapy.Request(url=postUrl, callback=self.parse_detail, headers=payloadHeader, meta={'payloadFlag': True, 'payloadData': payloadData, 'headers': payloadHeader})

    # 图数据接口
    def parse_result_detail(self, response):
        # url = f'https://sbcharts.investing.com/events_charts/us/{url_num}.json'
        # yield scrapy.Request(url=url, callback=self.parse_result_detail, meta={'items': Investing_item})
        country = response.meta['items']['country']
        theme = response.meta['items']['theme']
        theme = theme.replace('Investing.com', '')
        importance = response.meta['items']['importance']

        Investing_item = Investing_Item()
        Investing_item['country'] = country
        Investing_item['theme'] = theme
        Investing_item['importance'] = importance

        res = response.css('::text').extract_first().replace('null', '"null" ')
        attr_list = eval(res)['attr']
        for i in range(len(attr_list)-1, -1, -1):
            list_str = list(str(attr_list[i]['timestamp']))
            list_str.insert(-3, '/')
            str_a = ''.join(list_str).split('/')[0]

            # 爬到15年1月1号
            if int(str_a) < 1420041600:
                continue
            else:
                real_date, real_time = self.time_timestamp(str_a)
                e_Forecast = attr_list[i]['forecast_formatted']
                if i == -1:
                    b_value = ''
                else:
                    b_value = attr_list[i - 1]['actual_formatted']
                if i == len(attr_list)-1:
                    result = attr_list[i]['actual_formatted']
                else:
                    if attr_list[i]['revised_formatted'] == '':
                        result = attr_list[i]['actual_formatted']
                    else:
                        result = attr_list[i]['revised_formatted']

                e_Forecast, unit = save_unit(e_Forecast)
                b_value, unit = save_unit(b_value)
                result, unit = save_unit(result)

                Investing_item['real_date'] = real_date
                Investing_item['real_time'] = real_time
                Investing_item['result'] = result
                Investing_item['e_Forecast'] = e_Forecast
                Investing_item['b_value'] = b_value
                Investing_item['unit'] = unit
                yield Investing_item

                # # 写入excel
                # self.save_excel(country, theme, real_date, real_time, result, e_Forecast, b_value)

    # 写入excel
    def save_excel(self, country, theme, real_date, real_time, result, e_Forecast, b_value):
        # 写入excel
        list_ = []
        list_.append(country)
        list_.append(theme)
        list_.append(real_date)
        list_.append(real_time)
        list_.append(result)
        list_.append(e_Forecast)
        list_.append(b_value)
        excel_save().write_excel_xlsx('investing.xls', 'Sheet1', copy.deepcopy(list_))
        list_.clear()

    # 时间戳处理
    def time_timestamp(self, str_a):
        datatime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(str_a)))
        # datatime = datatime + '.' + str_a
        # print(datatime)
        r_date, r_time = str(datatime).split(' ')
        return r_date, r_time

    #判断是否为空
    def is_null(self, data):
        if data == '\xa0':
            data = ''
        else:
            data = data
        return data

    # 模拟点击
    def sele_click(self, url_num):
        # 模拟点击
        num = 0
        while True:
            # 点击翻页
            more_data = self.browser.find_element_by_xpath(f'//*[@id="showMoreHistory{url_num}"]/a').text
            # print(more_data)
            element1 = self.browser.find_element_by_xpath(f'//*[@id="showMoreHistory{url_num}"]/a')
            self.browser.execute_script("arguments[0].click();", element1)
            response = HtmlResponse(url=self.browser.current_url, body=self.browser.page_source, encoding="utf8")
            num += 1
            if more_data == None or num == 10:
                break
        return response

    # 转换货币单位
    def check_country(self, country):
        # '欧元区', '德国', '法国', '意大利', 'AUD', 'CAD', 'CNY', 'JPY', 'GBP', 'NZD', 'CHF', 'HKD', 'USD'
        if country == 'AUD':
            country = '澳大利亚'
        elif country == 'CAD':
            country = '加拿大'
        elif country == 'CNY':
            country = '中国'
        elif country == 'JPY':
            country = '日本'
        elif country == 'GBP':
            country = '英国'
        elif country == 'NZD':
            country = '新西兰'
        elif country == 'CHF':
            country = '瑞士'
        elif country == 'HKD':
            country = '香港'
        elif country == 'USD':
            country = '美国'
        return country