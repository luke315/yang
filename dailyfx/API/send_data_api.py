# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/5/22 14:49
# software: PyCharm

"""
文件说明：
"""
import datetime
import json
import time
from lib.api_args import dingding_url, yide_url_pro, yide_url_sit, yide_url_wechat_pro, yide_url_wechat_sit
from lib.mysql_setting import host, port, user, password, db
import pymysql
import requests
from lib.lib_args import current_time
import logging
import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)

# LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(pathname)s %(message)s "  # 配置输出日志格式
# DATE_FORMAT = '%Y-%m-%d  %H:%M:%S %a '  # 配置输出时间的格式，注意月份和天数不要搞乱了
# logging.basicConfig(level=logging.DEBUG,
#                     format=LOG_FORMAT,
#                     datefmt=DATE_FORMAT,
#                     filename=r"logs\%s-%s.log" % ('send_date', current_time),  # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
#                     )


class SendDataApi:
    # 记录请求接口时间
    data = ''
    method = ''

    # 发送易得实时数据
    def requests_api_update_yide(self):
        url_pro = yide_url_pro
        url_sit = yide_url_sit
        url_wc_pro = yide_url_wechat_pro
        url_wc_sit = yide_url_wechat_sit

        data = self.data['data']
        # 微信pro
        response_wc_pro = requests.post(url=url_wc_pro, json=data, timeout=5)

        # 请求返回参数及状态码
        response_pro = requests.post(url=url_pro, json=data, timeout=5)
        # response_sit = requests.post(url=url_sit, json=data, timeout=5)

        # 测试
        response_wc_sit = requests.post(url=url_wc_sit, json=data, timeout=5)

        # 获取接口返回参数
        # print(response.content)
        logging.debug('时间：%s；接口：%s；数据：%s；返回码：%s。' % (datetime.datetime.now(), url_pro, self.data, response_pro.status_code))
        logging.debug('时间：%s；接口：%s；数据：%s；返回码：%s。' % (datetime.datetime.now(), url_wc_pro, self.data, response_wc_pro.status_code))
        if response_pro.status_code == 200:
            return True
        else:
            # 请求失败发送成功邮件 
            self.send_msg()
            return False

    # 发送叮叮实时数据
    def requests_api_update_dingding(self, version):
        url = dingding_url[0]
        # 请求返回参数及状态码
        response = requests.post(url=url, json=self.data)

        # 获取接口返回参数
        res = json.loads(response.content.decode('utf8'))
        '''
        返回参数格式
                    {
                        "code": 1,
                        "msg": "success",
                        "time": "1590673328",
                        "data": []
                    }
        '''
        # print(response.content)
        self.save_mysql(current_time, current_time, version, 'dingding', 'post', url, res['code'], 'T_dingding')
        logging.debug('时间：%s；接口：%s；数据：%s；返回码：%s。' % (datetime.datetime.now(), url, self.data, res['code']))
        if res['code'] == 1:
            pass
        else:
            # 请求失败发送成功邮件
            self.send_msg()
            pass

    # 发送叮叮预测数据
    def requests_api_forecast(self, push_date_start, push_date_end, version):
        url = dingding_url[1]
        i = 1
        while i < 2:
            # 请求返回参数及状态码
            response = requests.post(url=url, json=self.data)
            res = json.loads(response.content.decode('utf8'))
            '''
                        {
                "code": 0,
                "msg": "签名验证失败",
                "time": "1590673328",
                "data": []
            }
            '''
            # 获取接口返回参数
            logging.debug('时间：%s；接口：%s；数据：%s；返回码：%s。' % (datetime.datetime.now(), url, self.data, res['code']))

            if push_date_start == push_date_end:
                type_api = 'F1_dingding'
            else:
                type_api = 'F7_dingding'
            self.save_mysql(push_date_start, push_date_end, version, 'dingding', 'post', url, res['code'], type_api)
            if res['code'] == 1:
                return True
            else:
                # 请求失败发送成功邮件
                self.send_msg()
                i += 1
                time.sleep(25200)

    # 发送邮件配置
    def send_msg(self):
        import yagmail
        user = 'leiyang@irosetta.com'  # 邮箱用户名
        passwd = 'i78aqTCq3fgdUtmG'  # 邮箱授权码
        smtp_host = 'smtp.exmail.qq.com'  # 邮箱服务器地址
        mail = yagmail.SMTP(user=user, password=passwd, host=smtp_host)  # 连接上邮箱
        # to表示发给谁，cc表示抄送给谁,contents表示邮件主题，attachments表示附件路径
        mail.send(to=['liuyunsong@irosetta.com', 'leiyang@irosetta.com'],
                  subject=f'{self.method}请求状态',
                  contents='请求时间：%s；\n 请求状态：%s；\n 请求携带数据：%s；\n' % (datetime.datetime.now(), '请求失败', self.data), )

    def main(self, data, *args, **kwargs):
        self.data = data
        method_ = args[0]
        self.method = method_
        version = args[1]
        if method_ == 'yide':
            # 0点发数据易得
            try:
                self.requests_api_update_yide()
            except Exception as e:
                print(e)
        elif method_ == 'T':
            # 实时更新合作数据
            try:
                self.requests_api_update_dingding(version)
            except Exception as e:
                print(e)
        elif method_ == 'F':
            push_date_start = kwargs['push_date_start']
            push_date_end = kwargs['push_date_end']
            # print('预测数据')
            try:
                # 定时调用
                self.requests_api_forecast(push_date_start, push_date_end, version)
            except Exception as e:
                print(e)

    # 保存推送记录
    def save_mysql(self, push_date_start, push_date_end, the_version, comsumer, requests_type, requests_url, status, type_api):
        conn = pymysql.Connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db)
        cursor = conn.cursor()
        sql = """insert into push_record (push_date_start, push_date_end, the_version, comsumer, requests_type, requests_url, status, type_api) values("%s","%s","%s","%s","%s","%s","%s", "%s")""" \
              % (push_date_start, push_date_end, the_version, comsumer, requests_type, requests_url, status, type_api)
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
        conn.close()
