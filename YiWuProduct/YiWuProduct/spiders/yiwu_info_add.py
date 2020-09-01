# -*- coding: utf-8 -*-
import re
import scrapy
from YiWuProduct.items import YiWuItem, ValueItem
from lib.lib_args import current_time

dict_data = {'index': 1}

class Yiwu_add_Spider(scrapy.Spider):
    name = 'yiwu_info_add'
    base_url = 'http://www.ywindex.com'
    frequency = ''
    custom_settings = {
        'ITEM_PIPELINES': {'YiWuProduct.pipelines.Yiwu_Postgre_Pipeline': 300},

        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': '././Logs/%s.%s.log' % (name, current_time)
    }

    def __init__(self, frequency=None, *args, **kwargs):
        super(Yiwu_add_Spider, self).__init__(*args, **kwargs)
        self.frequency = frequency

    def start_requests(self):
        url = 'http://www.ywindex.com/Home/Product/index/'
        yield scrapy.Request(url=url, callback=self.parse)

    # 主页面， 获取指标父节点code和name
    def parse(self, response):
        yiwu_item = YiWuItem()
        # 子页
        div_list = response.css('.aui-content-main>div')
        for div in div_list:
            name = div.css('.aui-content-menu-head-list>a  ::text').extract_first().replace(' ', '')
            parent_url = div.css('.aui-content-menu-head-list>a  ::attr(href)').extract_first()
            parent_c = parent_url.split('/')[-1].split('.')[0]
            # 父节点参数
            yiwu_item['parent_code'] = 'start'
            yiwu_item['code'] = parent_c
            yiwu_item['name'] = name
            yiwu_item['frequency'] = ''
            yield yiwu_item
            url = self.base_url + parent_url
            yield scrapy.Request(url=url, callback=self.parse_detail)

        # 首页
        yiwu_item['code'] = 'start'
        yiwu_item['parent_code'] = 'startparent'
        yiwu_item['name'] = '义乌小商品指数'
        yiwu_item['frequency'] = ''
        yield yiwu_item

        yiwu_item['parent_code'] = 'start'
        yiwu_item['code'] = '00'
        yiwu_item['name'] = '总指数'
        yiwu_item['frequency'] = ''
        yield yiwu_item

        yield scrapy.Request(url=response.request.url, callback=self.epx_value, meta={'items': yiwu_item})

    def parse_detail(self, response):
        parent_code = response.request.url.split('/')[-1].split('.')[0]
        yiwu_item = YiWuItem()
        div_list = response.css('.ny_mian_right>div')[1].css('div')
        # 判断是否为叶子几点
        parent_url = response.request.url.replace(f'{self.base_url}', '')
        url_list = response.css('.ny_mian_right>div')[1].css('div>a ::attr(href)').extract()

        parent_name = response.css('.ny_mian_right>div')[0].css('b ::text').extract_first()
        if len(parent_name) > 8:
            parent_name_8 = parent_name[0:8]
        else:
            parent_name_8 = parent_name
        name_list = response.css('.ny_mian_right>div')[1].css('div>a ::text').extract()

        if parent_url not in url_list and parent_name_8 not in name_list:
            for a in div_list:
                parent_url = a.css('a ::attr(href)').extract_first()
                name = a.css('a ::text').extract_first()
                parent_c = parent_url.split('/')[-1].split('.')[0]
                yiwu_item['parent_code'] = parent_code
                yiwu_item['code'] = parent_c
                yiwu_item['name'] = name
                yiwu_item['frequency'] = ''
                url = self.base_url + parent_url
                yield scrapy.Request(url=url, callback=self.parse_detail, dont_filter=False)

        yiwu_item['name'] = parent_name
        yield scrapy.Request(url=response.request.url, dont_filter=True, callback=self.epx_value, meta={'items':yiwu_item})


    def epx_value(self, response):
        parent_name = response.meta['items']['name']
        html_ture = response.request.url
        # 周， 月，指数
        all_div_data = response.css('.tab-content>div')
        for i in all_div_data:  # 周价格指数/月价格指数/景气指数 div
            tablex_div = i.css('.tablex')[-1]  # 具体数据div
            if re.findall('.html', html_ture) != []:
                parent_code = html_ture.split('/')[-1].split('.')[0]
                # 子节点
                type_name = i.css('div')[2].css('::text').extract()[1].replace(' ', '')  # 周价格指数/月价格指数/景气指数
            else:
                parent_code = '00'
                type_name = i.css('div')[2].css('::text').extract()[0].replace(' ', '')  # 周价格指数/月价格指数/景气指数
            # 第一层
            if type_name == '景气指数':
                type_name = '月景气指数'
            yiwu_item = YiWuItem()
            yiwu_item['parent_code'] = parent_code
            yiwu_item['code'] = f"{parent_code}{self.change_name(type_name)}"
            yiwu_item['name'] = f'{parent_name}：{type_name}'
            yiwu_item['frequency'] = ''
            yield yiwu_item

            type_son_name_list = tablex_div.css('.table-row')[0].css('li>b ::text').extract()  # 场内/网上/订单 list
            type_theme_data = tablex_div.css('.table-footer-group>ul')  # ul_list

            for data_ul in type_theme_data:
                data = data_ul.css('li ::text').extract()
                for i_data in range(0, len(data)):
                    if i_data != 0:
                        the_date = data[0]
                        type_son_name = type_son_name_list[i_data]
                        value = data[i_data]

                        # save exp
                        yiwu_item['parent_code'] = f"{parent_code}{self.change_name(type_name)}"
                        yiwu_item['code'] = f"{parent_code}{self.change_name(type_name)}{self.md5_data(type_son_name)}"
                        yiwu_item['name'] = f'{parent_name}：{type_son_name}({type_name[0]})'
                        if re.findall('周', type_name) != []:
                            yiwu_item['frequency'] = 'WEEK'
                        else:
                            yiwu_item['frequency'] = 'MONTH'
                        yield yiwu_item

                        # save值
                        value_item = ValueItem()
                        value_item['the_date'] = the_date
                        value_item['value'] = value
                        value_item['code'] = f"{parent_code}{self.change_name(type_name)}{self.md5_data(type_son_name)}"
                        if re.findall('周', type_name) != []:
                            value_item['frequency'] = 'WEEK'
                        else:
                            value_item['frequency'] = 'MONTH'
                        yield value_item
                if self.frequency != None:
                    break

    def change_name(self, value):
        if value == '月景气指数':
            return 'MJQ'
        elif value == '周价格指数':
            return 'WJG'
        elif value == '月价格指数':
            return 'MJG'


    def md5_data(self, data):
        import hashlib
        res_value = hashlib.md5(data.encode(encoding='UTF-8')).hexdigest()
        return res_value