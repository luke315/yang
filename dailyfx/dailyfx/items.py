# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DailyfxItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class MyproItem(scrapy.Item):
    # define the fields for your item here like:
    real_date = scrapy.Field()
    country = scrapy.Field()
    real_time = scrapy.Field()
    theme = scrapy.Field()
    importance = scrapy.Field()
    result = scrapy.Field()
    result_unit = scrapy.Field()
    e_Forecast = scrapy.Field()
    e_Forecast_unit = scrapy.Field()
    b_value = scrapy.Field()
    b_value_unit = scrapy.Field()
    explan = scrapy.Field()


class Investing_Item(scrapy.Item):
    # define the fields for your item here like:
    theme = scrapy.Field()
    real_date = scrapy.Field()
    theme_time = scrapy.Field()
    real_time = scrapy.Field()
    country = scrapy.Field()
    result = scrapy.Field()
    result_unit = scrapy.Field()
    importance = scrapy.Field()
    e_Forecast = scrapy.Field()
    e_Forecast_unit = scrapy.Field()
    b_value = scrapy.Field()
    b_value_unit = scrapy.Field()
    type_theme = scrapy.Field()





    url_num = scrapy.Field()
