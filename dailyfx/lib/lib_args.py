# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/5/28 17:51
# software: PyCharm

"""
文件说明：
"""
import re

import pymysql
import datetime

# inversting 下载中间件判断url_list
url_list = []

# inversting 筛选货币
country_list = ['中国', '美国', '英国', '法国', '德国', '欧元区', '意大利', '澳大利亚', '加拿大', '日本', '新西兰',
                '瑞士', '香港', '阿根廷', '巴西', '印度', '印度尼西亚', '韩国', '墨西哥', '俄罗斯', '阿拉伯联合酋长国',
                '南非', '土耳其', '埃及', '泰国', '马来西亚', '越南', '西班牙', '新加坡']

# 时间参数
current_time = datetime.date.today()

# 当前时间版本参数
the_version = current_time


# 单位分隔函数
def save_unit(value):
    import re
    unit = re.findall('[\u4e00-\u9fa5]+|[a-zA-Z]+', value)
    if unit != []:
        unit = unit[0]
        result = value.replace('%s' % unit, '')
    else:
        result = value
        unit = re.findall(r'%', result)
        if unit != []:
            unit = unit[0]
            result = result.replace('%s' % unit, '')
        else:
            unit = ''
            result = result
    return result, unit


# 判断时间是否为讲话等
def get_theme_type(theme):
    list_theme_type = ['讲话', '会议', '纪要', '月报', '季报', '年报', '报告', '公报', '褐皮书', '白皮书', '蓝皮书', '发布会', '声明', '发言', '年会',
                       '评估', '预算', '决议', '听证会', '选举', '结果', '例会']
    for i in list_theme_type:
        if re.search(f'{i}', theme) != None:
            return True
    return False


def get_week_day(date):
    week_day_dict = {
        0: '星期一',
        1: '星期二',
        2: '星期三',
        3: '星期四',
        4: '星期五',
        5: '星期六',
        6: '星期天',
    }
    day = date.weekday()
    return week_day_dict[day]


host = '47.100.162.232'
port = 3306
user = 'root'
password = '1qaz@WSX'
db = 'crawl_spider_information'


# 获取公司指标,暂无用
class epx_data:
    conn = None

    def __init__(self):
        self.conn = pymysql.Connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db)

    # 获取公司指标
    def read_company_epx(self, company):
        cursor = self.conn.cursor()
        select_sql = """select theme from company_send_epx where company=%s"""
        cursor.execute(select_sql, company)
        result = cursor.fetchall()
        return result

    # 保存新公司指标
    def save_company_epx(self, company, list_theme):
        for theme in list_theme:
            return_epx = self.read_company_epx(company)
            # 如果指标存在则略过
            if theme in return_epx:
                pass
            else:
                # 不存在即添加新指标
                cursor = self.conn.cursor()
                select_sql = """insert into company_send_epx (company, theme) values("%s", "%s")""" % (company, theme)
                try:
                    cursor.execute(select_sql)
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    self.conn.rollback()
