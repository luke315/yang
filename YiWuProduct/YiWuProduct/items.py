# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class YiwuproductItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class YiWuItem(scrapy.Item):
    parent_code = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()
    frequency = scrapy.Field()

class ValueItem(scrapy.Item):
    code = scrapy.Field()
    value = scrapy.Field()
    the_date = scrapy.Field()
    frequency = scrapy.Field()

