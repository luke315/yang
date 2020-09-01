
import os
import datetime
import time
import schedule
import threading


def investing_com_T():
    print("I'm working for investing_com_T ")
    os.system("scrapy crawl investing_com_T --nolog")
    print("investing_com_T:", datetime.datetime.now())

def investing_com_F():
    print("I'm working for investing_com_F")
    os.system("scrapy crawl investing_com_F --nolog")
    print("investing_com_F:", datetime.datetime.now())

def investing_com_T_task():
    threading.Thread(target=investing_com_T).start()

def investing_com_F_task():
    threading.Thread(target=investing_com_F).start()


def run():
    print('*' * 10 + 'intesting爬虫開始' + '*' * 10)
    schedule.every().day.at("00:00").do(investing_com_T_task)
    schedule.every().day.at("10:00").do(investing_com_F_task)
    print('*' * 10 + '定时爬虫开始运行' + '*' * 10)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    run()