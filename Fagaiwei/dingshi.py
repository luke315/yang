# import subprocess
# import schedule
# import time
# import datetime
# from multiprocessing import Process
import os
import datetime
import time
import schedule
import threading

from lib.delete_log import delete_logs_star


def job1():
    os.system("scrapy crawl s_z_stock_info")

def job1_task():
    threading.Thread(target=job1).start()

def job2():
    os.system("scrapy crawl hk_stock_info")

def job2_task():
    threading.Thread(target=job2).start()

def job3():
    os.system("scrapy crawl American_stock_info")

def job3_task():
    threading.Thread(target=job3).start()

def job4():
    os.system("scrapy crawl hk_stock")

def job4_task():
    threading.Thread(target=job4).start()

def job5():
    os.system("scrapy crawl US_ChinaStock_info")

def job5_task():
    threading.Thread(target=job5).start()


def job6():
    print('保留三天log')
    delete_logs_star()

def job6_task():
    threading.Thread(target=job6).start()

def job7():
    os.system("scrapy crawl ETF_market_info")

def job7_task():
    threading.Thread(target=job7).start()

def job8():
    os.system("scrapy crawl ETF_market_info_history")

def job8_task():
    threading.Thread(target=job8).start()


def run():
    print('*' * 10 + '开始执行定时爬虫' + '*' * 10)
    # # 每天爬新增上市（沪深）
    schedule.every().day.at("23:50").do(job1_task)

    # 港股定时
    schedule.every().day.at("16:30").do(job2_task)

    # 港股root表更新
    schedule.every().day.at("17:30").do(job4_task)

    # 美股定时
    schedule.every().day.at("05:30").do(job3_task)
    # 美股root表更新
    schedule.every().day.at("06:30").do(job5_task)

    # ETF定时
    schedule.every().day.at("16:30").do(job7_task)
    schedule.every().day.at("03:30").do(job7_task)

    # 每周爬一次歷史數據
    schedule.every().sunday.at("08:30").do(job6_task)

    # 每周日删除log
    # schedule.every().friday.at("00:30").do(job6_task)

    print('当前时间为{}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print('*' * 10 + '定时爬虫开始运行' + '*' * 10)
    while True:
        time.sleep(5)
        schedule.run_pending()


if __name__ == '__main__':
    run()