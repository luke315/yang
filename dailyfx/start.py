

from scrapy.cmdline import execute

# 英为数据
# execute('scrapy crawl investing_com '.split())
# execute('scrapy crawl investing_com --nolog'.split())

execute('scrapy crawl investing_com_T'.split())
# execute('scrapy crawl investing_com_T --nolog'.split())

# execute('scrapy crawl investing_com_F'.split())
# # execute('scrapy crawl investing_com_F --nolog'.split())

# 财经日历
# from scrapy.cmdline import execute
# execute('scrapy crawl dailyfx --nolog'.split())