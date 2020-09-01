# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/5/19 20:53
# software: PyCharm

"""
文件说明：
"""

from scrapy.cmdline import execute


# execute(['scrapy', 'crawl', 's_z_stock_pdf_info'])

# execute(['scrapy', 'crawl', 's_z_stock_info'])


# execute(['scrapy', 'crawl', 'hk_stock'])
# execute(['scrapy', 'crawl', 'hk_stock_info'])


# execute(['scrapy', 'crawl', 'American_stock'])
# execute(['scrapy', 'crawl', 'American_stock_info'])

# execute(['scrapy', 'crawl', 'US_ChinaStock_info'])

execute(['scrapy', 'crawl', 'ETF_market_info'])
# execute(['scrapy', 'crawl', 'ETF_market_info_history'])