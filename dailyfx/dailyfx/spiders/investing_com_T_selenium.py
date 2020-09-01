# -*- coding: utf-8 -*-
import re
from threading import Thread
import scrapy
from dailyfx.items import Investing_Item
from lib.lib_args import save_unit
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

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


class InvestingComTSpider(scrapy.Spider):
    name = 'investing_com_T_selenium'
    start_urls = ['https://cn.investing.com/economic-calendar/']

    custom_settings = {
        # 'ITEM_PIPELINES': {'dailyfx.pipelines.Investing_Item_Pipeline': 300},

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

    def parse(self, response):
        date_time = {'date': ''}
        tr_list = response.css('#economicCalendarData>tbody>tr')
        for tr in tr_list:
            td_list = tr.css('td')
            if len(td_list) == 1:
                date_time['date'] = tr.css('td ::text').extract_first().split(' ')[0] \
                    .replace('年', '-').replace('月', '-').replace('日', '').split('\xa0\xa0')[0]

            else:
                real_date = date_time['date']
                real_time = td_list[0].css('::text').extract_first()
                country = td_list[1].xpath('span/@title').extract_first()
                if real_time == '全天':
                    importance = ''
                    result = ''
                    e_Forecast = ''
                    b_value = ''
                    theme = td_list[3].css('a ::text').extract_first()
                    type_theme = ''
                    theme_time = ''
                else:
                    importance_list = td_list[2].css('i ::attr(class)').extract()
                    # 去除空字符
                    result = td_list[4].css('::text').extract_first()
                    result = self.is_null(result)
                    e_Forecast = td_list[5].css('::text').extract_first()
                    e_Forecast = self.is_null(e_Forecast)
                    b_value = td_list[6].css(' ::text').extract_first()
                    b_value = self.is_null(b_value)
                    if importance_list[2] == 'grayFullBullishIcon':
                        importance = '高'
                    elif importance_list[1] == 'grayFullBullishIcon':
                        importance = '中'
                    else:
                        importance = '低'

                    # 获取指标唯一url
                    url = td_list[3].css('a ::attr(href)').extract_first()
                    type_theme = td_list[3].css('a ::attr(href)').extract_first().split('/')[-1]
                    # 获取指标名
                    # 异步获取
                    # theme = theme('https://cn.investing.com' + url)

                    # 同步获取
                    theme = self.theme('https://cn.investing.com' + url)

                    # 获指标发布时间
                    theme_text = td_list[3].css('a ::text').extract_first()
                    theme_time = self.check_theme_time(theme_text)

                    # 判断指标是否为初值
                    epx_time = tr.css('.smallGrayP ').extract_first()
                    if epx_time != None:
                        theme_time = '初值'

                # 分离单位
                e_Forecast, e_Forecast_unit = save_unit(e_Forecast)
                b_value, b_value_unit = save_unit(b_value)
                result, result_unit = save_unit(result)

                Investing_item = Investing_Item()
                Investing_item['real_date'] = real_date
                Investing_item['real_time'] = real_time
                Investing_item['country'] = country
                Investing_item['theme'] = theme
                Investing_item['type_theme'] = type_theme
                Investing_item['theme_time'] = theme_time
                Investing_item['importance'] = importance
                Investing_item['result'] = result
                Investing_item['e_Forecast'] = e_Forecast
                Investing_item['b_value'] = b_value
                Investing_item['result_unit'] = result_unit
                Investing_item['b_value_unit'] = b_value_unit
                Investing_item['e_Forecast_unit'] = e_Forecast_unit
                print(Investing_item)
                # yield Investing_item

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
            if re_season != None:
                theme_time = res_list[-1]
            elif re_mouth != None:
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

