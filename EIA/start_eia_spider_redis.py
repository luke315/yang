# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/5/19 20:53
# software: PyCharm

"""
文件说明：
"""

from scrapy.cmdline import execute

# execute(['scrapy', 'crawl', 'eia_spider', '-s', 'JOBDIR=crawl/continue'])
# execute(['scrapy', 'crawl', 'eia_spider'])

# execute(['scrapy', 'crawl', 'eia_add_api', '-a', 'frequency=YEAR'])
# execute(['scrapy', 'crawl', 'eia_add_api', '-a', 'frequency=SEASON'])
# execute(['scrapy', 'crawl', 'eia_add_api', '-a', 'frequency=MONTH'])
# execute(['scrapy', 'crawl', 'eia_add_api', '-a', 'frequency=WEEK'])
# execute(['scrapy', 'crawl', 'eia_add_api', '-a', 'frequency=DATE'])
# execute(['scrapy', 'crawl', 'eia_add_api', '-a', 'frequency=4'])

# add
# execute(['scrapy', 'crawl', 'eia_add', '-a', 'frequency=YEAR'])
# execute(['scrapy', 'crawl', 'eia_add', '-a', 'frequency=SEASON'])
# execute(['scrapy', 'crawl', 'eia_add', '-a', 'frequency=MONTH'])
# # execute(['scrapy', 'crawl', 'eia_add', '-a', 'frequency=WEEK'])
# execute(['scrapy', 'crawl', 'eia_add', '-a', 'frequency=DATE'])


execute(['scrapy', 'crawl', 'eia_redis'])