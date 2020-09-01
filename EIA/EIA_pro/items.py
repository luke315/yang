# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Eia_Epx_Item(scrapy.Item):
    # define the fields for your item here like:
    # ext1 = scrapy.Field()  # 单位
    # ext3 = scrapy.Field()  # 频率
    # ext4 = scrapy.Field()  # 版本
    
    epx_name = scrapy.Field()  # 目录名字
    epx_code = scrapy.Field()  # ID
    epx_parent_code = scrapy.Field()  # 父级ID
    epx_frequency = scrapy.Field()  # 频率
    epx_units = scrapy.Field()  # 单位

class Eia_Basic_Item(scrapy.Item):
    epx_code = scrapy.Field()
    # series_name = scrapy.Field()  # 类别名称
    period = scrapy.Field()  # 时期
    frequency = scrapy.Field()  # 频率
    epx_value = scrapy.Field()  # 值
    # units = scrapy.Field()  # 单位



'''
series_name = td.css('td')[0]
period = td.css('td')[1]
frequency = td.css('td')[2]
value = td.css('td')[3]
units = td.css('td')[4]
'''