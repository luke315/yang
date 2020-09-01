# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/5/29 11:33
# software: PyCharm

"""
文件说明：
"""

import time
import hashlib
import random
import string


# 开盘叮叮接口 索引1为预测数据接口，索引0为实时接口
# dingding_url = ['http://127.0.0.1:8000/test/', 'http://127.0.0.1:8000/test/']

# 叮叮接口
#测试环境
# dingding_url = ['http://marketopen.rootant.com/api/lstapi/updateevent', 'http://marketopen.rootant.com/api/lstapi/notice']

# 正式环境
dingding_url = ['https://marketopen.cn/api/lstapi/updateevent', 'https://marketopen.cn/api/lstapi/notice']


# pc易得pro
yide_url_pro = 'http://www.1999data.com/other/getEconomicCalendar'

# pc易得sit
yide_url_sit = 'http://sit.1999data.com/other/getEconomicCalendar'

# wechat易得pro
yide_url_wechat_pro = 'http://yide-wechat.1999data.com/other/getEconomicCalendar'

# wechat易得sit
yide_url_wechat_sit = 'http://sit.yide-wechat.1999data.com/other/getEconomicCalendar'

# 本地
# yide_url = 'http://192.168.0.102:10075/other/getEconomicCalendar'


def dingding_sign_md5():
    # 随机字符串
    nonce = ''.join(random.sample(string.ascii_letters + string.digits, 32))
    # 时间戳
    timestamp = str(int(time.time()))
    # 密钥
    secretkey = '6aUXKXynP2Puk9Uk'
    stringA = 'nonce=%s&timestamp=%s' % (nonce, timestamp)
    stringSignTemp = '%s&secretkey=%s' % (stringA, secretkey)
    # 加密签名
    signature = hashlib.md5(stringSignTemp.encode(encoding='UTF-8')).hexdigest().upper()
    return signature, timestamp, nonce

'''
正式环境：https://marketopen.cn
测试环境：http://marketopen.rootant.com
预告接口：/api/lstapi/notice
实时更新接口：/api/lstapi/updateevent

timestamp=时间戳
secretkey：6aUXKXynP2Puk9Uk  (密钥)
nonce=32位随机字符串
'''



# 易得接口
'''
local： http://192.168.0.102:10075/other/getEconomicCalendar
local： http://192.168.0.116:10076/other/getEconomicCalendar
sit:       http://sit.1999data.com/other/getEconomicCalendar
pro:     http://1999data.com/other/getEconomicCalendar
'''



# if __name__ == '__main__':
#     signature, timestamp, nonce = dingding_sign_md5()
#     print(signature, timestamp, nonce)