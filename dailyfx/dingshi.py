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


def job1():
    print("I'm working for job1 dailyfx")
    os.system("scrapy crawl dailyfx")
    print("job1 dailyfx:", datetime.datetime.now())


def job2():
    print("I'm working for job2 dailyfx_after")
    os.system("scrapy crawl dailyfx_after")
    print("job2 dailyfx_after:", datetime.datetime.now())


def job3():
    print("I'm working for dailyfx_after30")
    os.system("scrapy crawl dailyfx_after30")
    print("dailyfx_after30:", datetime.datetime.now())


def job1_task():
    threading.Thread(target=job1).start()


def job2_task():
    threading.Thread(target=job2).start()


def job3_task():
    threading.Thread(target=job3).start()


def run():
    print('*' * 10 + '开始执行定时爬虫' + '*' * 10)
    
    schedule.every().day.at("00:00").do(job1_task)
    # schedule.every().friday.at("10:00").do(job2_task)
    # schedule.every().day.at("00:00").do(job3_task)

    print('当前时间为{}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print('*' * 10 + '定时爬虫开始运行' + '*' * 10)
    while True:
        time.sleep(0.5)
        schedule.run_pending()


if __name__ == '__main__':
    run()