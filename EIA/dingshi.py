# import subprocess
# import schedule
# import time
# import datetime
# from multiprocessing import Process
import os
import time
import schedule
import threading
import datetime

def task_year():
    print("working for task_year")
    os.system("scrapy crawl eia_add_api -a frequency=YEAR")
    print("YEAR task finish", datetime.datetime.now())

def task_season():
    print("working for task SEASON")
    os.system("scrapy crawl eia_add_api -a frequency=SEASON")
    print("SEASON task finish", datetime.datetime.now())

def task_month():
    print("working for task MONTH")
    os.system("scrapy crawl eia_add_api -a frequency=MONTH")
    print("MONTH task finish", datetime.datetime.now())

def task_week():
    print("working for task week")
    os.system("scrapy crawl eia_add_api -a frequency=WEEK")
    print("WEEK task finish", datetime.datetime.now())

def task_date():
    print("working for task DATE")
    os.system("scrapy crawl eia_add_api -a frequency=DATE")
    print("DATE task finish", datetime.datetime.now())



def job_task_year():
    threading.Thread(target=task_year).start()

def job_task_quarter():
    threading.Thread(target=task_season).start()

def job_task_month():
    threading.Thread(target=task_month).start()

def job_task_week():
    threading.Thread(target=task_week).start()

def job_task_day():
    threading.Thread(target=task_date).start()


def run():
    print('*' * 10 + '开始执行定时爬虫' + '*' * 10)
    schedule.every().week.do(job_task_week)
    schedule.every().day.at("00:00").do(job_task_day)
    print('当前时间为{}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print('*' * 10 + '定时爬虫开始运行' + '*' * 10)
    while True:
        time.sleep(0.5)
        schedule.run_pending()

def dingshi():
    print('*' * 10 + '开始执行定时爬虫' + '*' * 10)
    while True:
        # 当前日期
        date_day = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        now = datetime.datetime.now()
        # 日度
        date_start = datetime.datetime.now().strftime('%H:%M')
        # 周度
        this_week_start = str(now - datetime.timedelta(days=now.weekday())).split(' ')[0] + ' 10:01'
        # 月度
        this_month_start = datetime.datetime(now.year, now.month, 1).strftime('%Y-%m-%d') + ' 10:01'
        # 季度
        month = (now.month - 1) - (now.month - 1) % 3 + 1
        this_quarter_start = datetime.datetime(now.year, month, 1).strftime('%Y-%m-%d') + ' 10:01'
        # 年度
        this_year_start = datetime.datetime(now.year, 1, 1).strftime('%Y-%m-%d') + ' 10:01'
        # print(date_start)
        # print(this_week_start)
        # print(this_month_start)
        # print(this_quarter_start)
        # print(this_year_start)
        if date_start == '00:00':
            job_task_day()
            time.sleep(60)
        elif date_day == this_week_start:
            job_task_week()
            time.sleep(60)
        elif date_day == this_month_start:
            job_task_month()
            time.sleep(60)
        elif date_day == this_quarter_start:
            job_task_quarter()
            time.sleep(60)
        elif date_day == this_year_start:
            job_task_year()
            time.sleep(60)
        else:
            time.sleep(60)

if __name__ == '__main__':
    # run()
    dingshi()

