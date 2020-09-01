# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/5/28 17:49
# software: PyCharm

"""
文件说明：
"""
import copy
import datetime
from lib.api_args import dingding_sign_md5, dingding_url
from lib.lib_args import save_unit, get_theme_type
from lib.mysql_setting import host, port, user, password, db
import pymysql
from lib.lib_args import current_time
from API.send_data_api import SendDataApi
from threading import Thread

# 总数据字典
data_all = {}



# 异步调用
def async_func(f):
    def wrapper(*args, **kwargs):
        thr = Thread(target=f, args=args, kwargs=kwargs)
        thr.start()
    return wrapper


def conect_sql():
    conn = pymysql.Connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=db)
    conn.ping(reconnect=True)
    cursor = conn.cursor()
    return cursor


# 易得实时接口2.0
# @async_func
def send_1999data(send_list):
    data_all_yide = {}
    from API.send_data_api import SendDataApi
    data_all_yide['%s' % (str(current_time).replace('-', '/'))] = send_list
    data = {}
    data['data'] = data_all_yide
    # print(data)
    SendDataApi().main(data, 'yide', '1')


# 实时数据接口（合作）
# @async_func
def get_data_day_dingding(spider_name, real_date, theme_value, result_value, r_time):
    real_date1 = real_date.replace('-', '/', 2)
    # 获取版本
    ver = get_the_version(real_date)[0]
    if spider_name == 'dailyfx':
        # 根据日期获取版本
        conn = pymysql.Connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db)
        conn.ping(reconnect=True)
        cursor = conn.cursor()
        # 防止转义
        date_theme = pymysql.escape_string(real_date1 + theme_value)

        # 链接数据库，判断事件是否存在
        select_sql = """SELECT * FROM event_calendar WHERE date_theme = %s AND the_version=%s"""
        cursor.execute(select_sql, (date_theme, ver))
        result = cursor.fetchone()
        if result is None:
            pass
        else:
            result, result_unit = save_unit(result_value)
            if r_time == '':
                r_time = '00:00'
            # 存在即更新时间结果
            sql = """update event_calendar set real_time=%s, result = %s ,result_unit = %s where date_theme =%s AND the_version= %s"""
            try:
                cursor.execute(sql, (r_time, result, result_unit, date_theme, ver))
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
            # 有结果，则根据日期和版本，去拿数据库拿数据
            cursor = conn.cursor()
            select_sql = """SELECT real_date, real_time, country, theme, importance, result, result_unit, e_Forecast, e_Forecast_unit, b_value, b_value_unit, explan FROM event_calendar WHERE real_date =%s AND the_version=%s"""
            cursor.execute(select_sql, (real_date, ver))
            result = cursor.fetchall()
            list_ = get_sql_data(result, '', 'daliyfx')

            data_all[list_[0]['real_date']] = list_
            data = {}

            # 调用加密函数接口
            signature, timestamp, nonce = dingding_sign_md5()
            data['signature'] = signature
            data['timestamp'] = timestamp
            data['nonce'] = nonce
            data['data'] = data_all
            SendDataApi().main(data, 'T', ver)
    elif spider_name == 'investing_com_T':
        # 判断时间是否为叮叮所需事件：
        conn = pymysql.Connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db)
        conn.ping(reconnect=True)
        cursor = conn.cursor()

        res = get_company_theme('dingding', theme_value)
        theme = pymysql.escape_string(theme_value)
        real_date = real_date.replace('/', '-', 2)
        res1 = get_theme_type(theme_value)
        if res or res1 or r_time == '':
            select_sql = """SELECT * FROM investing WHERE real_date=%s AND theme =%s AND the_version= %s"""
            cursor.execute(select_sql, (real_date, theme, ver))
            result = cursor.fetchone()
            if result != None:
                result, result_unit = save_unit(result_value)
                api = 'dingding'
                # 存在即更新时间结果
                sql = """update investing set real_time=%s, result = %s ,result_unit = %s where real_date=%s AND theme =%s AND the_version= %s AND api=%s"""
                try:
                    cursor.execute(sql, (r_time, result, result_unit, real_date, pymysql.escape_string(theme_value), ver, api))
                    conn.commit()
                except Exception as e:
                    print(e)
                    conn.rollback()
                select_sql = """SELECT real_date, real_time, country, theme, theme_time, importance, result, result_unit, e_Forecast, e_Forecast_unit, b_value, b_value_unit, explan FROM investing WHERE real_date =%s AND the_version=%s AND api=%s"""
                cursor.execute(select_sql, (real_date, ver, api))
                result = cursor.fetchall()
                list_ = get_sql_data(result, '', 'investing')

                data_all[list_[0]['real_date']] = list_
                data = {}

                # 调用加密函数接口
                signature, timestamp, nonce = dingding_sign_md5()
                data['signature'] = signature
                data['timestamp'] = timestamp
                data['nonce'] = nonce
                data['data'] = data_all
                # print(data)
                SendDataApi().main(data, 'T', ver)


# sql当天数据整理
def get_sql_data(result, S_time, source):
    if source == 'daliyfx':
        if S_time != ' ':
            # 七天数据
            day1 = str(current_time + S_time).replace('-', '/')
            list1 = []
            day2 = str(current_time + S_time + (datetime.timedelta(days=1))).replace('-', '/')
            list2 = []
            day3 = str(current_time + S_time + (datetime.timedelta(days=2))).replace('-', '/')
            list3 = []
            day4 = str(current_time + S_time + (datetime.timedelta(days=3))).replace('-', '/')
            list4 = []
            day5 = str(current_time + S_time + (datetime.timedelta(days=4))).replace('-', '/')
            list5 = []
            day6 = str(current_time + S_time + (datetime.timedelta(days=5))).replace('-', '/')
            list6 = []
            day7 = str(current_time + S_time + (datetime.timedelta(days=6))).replace('-', '/')
            list7 = []
            for i in result:
                real_date, real_time, country, theme, importance, result, result_unit, e_Forecast, e_Forecast_unit, b_value, b_value_unit, explan = i
                if importance == '':
                    importance = '高'
                else:
                    pass
                if int(str(real_time).split(':')[0]) < 10:
                    # 处理事件格式
                    real_time = '0' + str(real_time).split(':')[0] + ':' + str(real_time).split(':')[1]
                    if real_time == '00:00':
                        real_time = ''

                    dict_ = {'b_value': b_value,
                             'b_value_unit': b_value_unit,
                             'country': country,
                             'e_Forecast': e_Forecast,
                             'e_Forecast_unit': e_Forecast_unit,
                             'explan': explan,
                             'importance': importance,
                             'real_date': str(real_date).replace('-', '/'),
                             'real_time': real_time,
                             'result_unit': result_unit,
                             'result': result,
                             'theme': theme}
                else:
                    dict_ = {'b_value': b_value,
                             'b_value_unit': b_value_unit,
                             'country': country,
                             'e_Forecast': e_Forecast,
                             'e_Forecast_unit': e_Forecast_unit,
                             'explan': explan,
                             'importance': importance,
                             'real_date': str(real_date).replace('-', '/'),
                             'real_time': str(real_time).split(':')[0] + ':' + str(real_time).split(':')[1],
                             'result': result,
                             'result_unit': result_unit,
                             'theme': theme}
                if day1 == str(real_date).replace('-', '/'):
                    list1.append(dict_)
                elif str(real_date).replace('-', '/') == day2:
                    list2.append(dict_)
                elif str(real_date).replace('-', '/') == day3:
                    list3.append(dict_)
                elif str(real_date).replace('-', '/') == day4:
                    list4.append(dict_)
                elif str(real_date).replace('-', '/') == day5:
                    list5.append(dict_)
                elif str(real_date).replace('-', '/') == day6:
                    list6.append(dict_)
                elif str(real_date).replace('-', '/') == day7:
                    list7.append(dict_)
                data_all[day1] = list1
                data_all[day2] = list2
                data_all[day3] = list3
                data_all[day4] = list4
                data_all[day5] = list5
                data_all[day6] = list6
                data_all[day7] = list7
            return data_all, day1, day7
        else:
            # 一天数据
            list_ = []
            # 调整待发送数据
            for i in result:
                real_date, real_time, country, theme, importance, result, result_unit, e_Forecast, e_Forecast_unit, b_value, b_value_unit, explan = i
                # print(real_date, real_time, country, theme, importance, result, e_Forecast, b_value, explan)
                if importance == '':
                    importance = '高'
                else:
                    pass
                if int(str(real_time).split(':')[0]) < 10:
                    # 处理事件格式
                    real_time = '0' + str(real_time).split(':')[0] + ':' + str(real_time).split(':')[1]
                    if real_time == '00:00':
                        real_time = ''

                    dict_ = {'b_value': b_value,
                             'b_value_unit': b_value_unit,
                             'country': country,
                             'e_Forecast': e_Forecast,
                             'e_Forecast_unit': e_Forecast_unit,
                             'explan': explan,
                             'importance': importance,
                             'real_date': str(real_date).replace('-', '/'),
                             'real_time': real_time,
                             'result_unit': result_unit,
                             'result': result,
                             'theme': theme}
                else:
                    dict_ = {'b_value': b_value,
                             'b_value_unit': b_value_unit,
                             'country': country,
                             'e_Forecast': e_Forecast,
                             'e_Forecast_unit': e_Forecast_unit,
                             'explan': explan,
                             'importance': importance,
                             'real_date': str(real_date).replace('-', '/'),
                             'real_time': str(real_time).split(':')[0] + ':' + str(real_time).split(':')[1],
                             'result': result,
                             'result_unit': result_unit,
                             'theme': theme}
                list_.append(dict_)
            return list_
    elif source == 'investing':
        if S_time != '':
            # 七天数据
            day1 = str(current_time + S_time).replace('-', '/')
            list1 = []
            day2 = str(current_time + S_time + (datetime.timedelta(days=1))).replace('-', '/')
            list2 = []
            day3 = str(current_time + S_time + (datetime.timedelta(days=2))).replace('-', '/')
            list3 = []
            day4 = str(current_time + S_time + (datetime.timedelta(days=3))).replace('-', '/')
            list4 = []
            day5 = str(current_time + S_time + (datetime.timedelta(days=4))).replace('-', '/')
            list5 = []
            day6 = str(current_time + S_time + (datetime.timedelta(days=5))).replace('-', '/')
            list6 = []
            day7 = str(current_time + S_time + (datetime.timedelta(days=6))).replace('-', '/')
            list7 = []
            for i in result:
                real_date, real_time, country, theme, theme_time, importance, result, result_unit, e_Forecast, e_Forecast_unit, b_value, b_value_unit, explan = i
                if theme_time != '':
                    theme_time = f'({theme_time})'

                if importance == '':
                    importance = '高'
                if int(str(real_time).split(':')[0]) < 10:
                    # 处理事件格式
                    real_time = '0' + str(real_time).split(':')[0] + ':' + str(real_time).split(':')[1]
                    if real_time == '00:00':
                        real_time = ''

                    dict_ = {'b_value': b_value,
                             'b_value_unit': b_value_unit,
                             'country': country,
                             'e_Forecast': e_Forecast,
                             'e_Forecast_unit': e_Forecast_unit,
                             'explan': explan,
                             'importance': importance,
                             'real_date': str(real_date).replace('-', '/'),
                             'real_time': real_time,
                             'result_unit': result_unit,
                             'result': result,
                             'theme_time': theme_time,
                             'theme': theme}
                else:
                    dict_ = {'b_value': b_value,
                             'b_value_unit': b_value_unit,
                             'country': country,
                             'e_Forecast': e_Forecast,
                             'e_Forecast_unit': e_Forecast_unit,
                             'explan': explan,
                             'importance': importance,
                             'real_date': str(real_date).replace('-', '/'),
                             'real_time': str(real_time).split(':')[0] + ':' + str(real_time).split(':')[1],
                             'result': result,
                             'result_unit': result_unit,
                             'theme_time': theme_time,
                             'theme': theme}
                if day1 == str(real_date).replace('-', '/'):
                    list1.append(dict_)
                elif str(real_date).replace('-', '/') == day2:
                    list2.append(dict_)
                elif str(real_date).replace('-', '/') == day3:
                    list3.append(dict_)
                elif str(real_date).replace('-', '/') == day4:
                    list4.append(dict_)
                elif str(real_date).replace('-', '/') == day5:
                    list5.append(dict_)
                elif str(real_date).replace('-', '/') == day6:
                    list6.append(dict_)
                elif str(real_date).replace('-', '/') == day7:
                    list7.append(dict_)
                data_all[day1] = list1
                data_all[day2] = list2
                data_all[day3] = list3
                data_all[day4] = list4
                data_all[day5] = list5
                data_all[day6] = list6
                data_all[day7] = list7
            return data_all, day1, day7
        else:
            # 一天数据
            list_ = []
            # 调整待发送数据
            for i in result:
                real_date, real_time, country, theme, theme_time, importance, result, result_unit, e_Forecast, e_Forecast_unit, b_value, b_value_unit, explan = i
                # print(real_date, real_time, country, theme, importance, result, e_Forecast, b_value, explan)
                if theme_time != '':
                    theme_time = f'({theme_time})'

                if importance == '':
                    importance = '高'
                else:
                    pass
                if int(str(real_time).split(':')[0]) < 10:
                    # 处理事件格式
                    real_time = '0' + str(real_time).split(':')[0] + ':' + str(real_time).split(':')[1]
                    if real_time == '00:00':
                        real_time = ''

                    dict_ = {'b_value': b_value,
                             'b_value_unit': b_value_unit,
                             'country': country,
                             'e_Forecast': e_Forecast,
                             'e_Forecast_unit': e_Forecast_unit,
                             'explan': explan,
                             'importance': importance,
                             'real_date': str(real_date).replace('-', '/'),
                             'real_time': real_time,
                             'result_unit': result_unit,
                             'result': result,
                             'theme_time': theme_time,
                             'theme': theme}
                else:
                    dict_ = {'b_value': b_value,
                             'b_value_unit': b_value_unit,
                             'country': country,
                             'e_Forecast': e_Forecast,
                             'e_Forecast_unit': e_Forecast_unit,
                             'explan': explan,
                             'importance': importance,
                             'real_date': str(real_date).replace('-', '/'),
                             'real_time': str(real_time).split(':')[0] + ':' + str(real_time).split(':')[1],
                             'result': result,
                             'result_unit': result_unit,
                             'theme_time': theme_time,
                             'theme': theme}
                list_.append(dict_)
            return list_


# 根据公司获取事件指标
def get_company_theme(company, theme):
    company_theme = []
    conn = pymysql.Connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=db)
    cursor = conn.cursor()
    if theme == '':
        select_sql = """select theme from company_send_epx where company=%s """
        cursor.execute(select_sql, company)
        result = cursor.fetchall()
        for theme in result:
            company_theme.append(theme[0])
        return company_theme
    else:
        select_sql = """select theme from company_send_epx where company=%s and theme=%s """
        cursor.execute(select_sql, (company, theme))
        result = cursor.fetchone()
        if result != None:
            return True
        else:
            return False


# 获取实时更新版本
def get_the_version(real_date):
    cursor = conect_sql()
    select_sql = """SELECT the_version FROM push_record WHERE type_api=%s and requests_url=%s and %s BETWEEN push_date_start AND push_date_end order by create_time desc"""
    cursor.execute(select_sql, ('F1_dingding', dingding_url[1], real_date))
    result = cursor.fetchone()
    return result


# 周预测数据接口（合作）
def get_data_week(source):
    S_time = datetime.timedelta(days=3)
    E_time = datetime.timedelta(days=9)
    cursor = conect_sql()
    if source == 'daliyfx':
        select_sql = """SELECT real_date, real_time, country, theme, importance, result, result_unit, e_Forecast, e_Forecast_unit, b_value, b_value_unit, explan FROM event_calendar WHERE the_version=%s AND real_date between %s and %s"""
        cursor.execute(select_sql, (current_time, current_time + S_time, current_time + E_time))
        result = cursor.fetchall()
        data_all, day1, day7 = get_sql_data(result, S_time, 'daliyfx')
    else:
        select_sql = """SELECT real_date, real_time, country, theme, theme_time, importance, result, result_unit, e_Forecast, e_Forecast_unit, b_value, b_value_unit, explan FROM investing WHERE api=%s AND the_version=%s AND real_date between %s and %s"""
        cursor.execute(select_sql, ('dingding', current_time, current_time + S_time, current_time + E_time))
        result = cursor.fetchall()
        data_all, day1, day7 = get_sql_data(result, S_time, 'investing')

    data = {}
    # 调用加密函数接口
    signature, timestamp, nonce = dingding_sign_md5()
    data['signature'] = signature
    data['timestamp'] = timestamp
    data['nonce'] = nonce
    data['data'] = data_all
    # print(data)

    # 调用发送数据接口
    SendDataApi().main(data, 'F', current_time, push_date_start=day1, push_date_end=day7)


# T+1 预测数据
def get_data_T1(source):
    T_time = datetime.timedelta(days=1)
    real_date_send_data = current_time + T_time
    cursor = conect_sql()
    if source == 'daliyfx':
        select_sql = """SELECT real_date, real_time, country, theme, importance, result, result_unit, e_Forecast, e_Forecast_unit, b_value, b_value_unit, explan FROM event_calendar WHERE the_version=%s AND real_date=%s"""
        cursor.execute(select_sql, (current_time, real_date_send_data))
        result = cursor.fetchall()
        # 调整待发送数据
        list_ = get_sql_data(result, '', 'daliyfx')
    else:
        select_sql = """SELECT real_date, real_time, country, theme, theme_time, importance, result, result_unit, e_Forecast, e_Forecast_unit, b_value, b_value_unit, explan FROM investing WHERE the_version=%s AND real_date=%s AND api=%s"""
        cursor.execute(select_sql, (current_time, real_date_send_data, 'dingding'))
        result = cursor.fetchall()
        # 调整待发送数据
        list_ = get_sql_data(result, '', 'investing')

    if list_ == []:
        data_all['%s' % str(real_date_send_data).replace('-', '/')] = []
    else:
        data_all[list_[0]['real_date']] = list_
    data = {}
    # 调用加密函数接口
    signature, timestamp, nonce = dingding_sign_md5()
    data['signature'] = signature
    data['timestamp'] = timestamp
    data['nonce'] = nonce
    data['data'] = data_all
    # print(data)

    # 调用发送数据接口
    SendDataApi().main(data, 'F', current_time, push_date_start=real_date_send_data, push_date_end=real_date_send_data)

# 去除未更新事件
# @async_func
# def delete_theme(list_theme):
#     if list_theme == []:
#         pass
#     else:
#         send_theme = []
#         for theme_value in list_theme:
#             res = get_company_theme('dingding', theme_value)
#             if res:
#                 send_theme.append(theme_value)
#
#         data_all['delete_theme'] = send_theme
#         data = {}
#         # 调用加密函数接口
#         signature, timestamp, nonce = dingding_sign_md5()
#         data['signature'] = signature
#         data['timestamp'] = timestamp
#         data['nonce'] = nonce
#         data['data'] = data_all
#         print(data)
#         # SendDataApi().main(data, 'T', '')
#
if __name__ == '__main__':
    # get_data_week('investing_com')
    get_data_day_dingding('investing_com_T', '2020-08-11', '英国失业金申请人数', '94.4K', '14:00')
    # list_1 = [{'real_date': '2020-8-11', 'real_time': '04:00', 'country': '美国', 'theme': '美国芝加哥联储主席埃文斯(CharlesEvans)讲话', 'theme_time': '', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '', 'result_unit': '', 'b_value_unit': '', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '06:45', 'country': '新西兰', 'theme': '新西兰电子卡零售销售月率', 'theme_time': '七月', 'importance': '中', 'result': '1.1', 'e_Forecast': '', 'b_value': '15.6', 'result_unit': '%', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '06:45', 'country': '新西兰', 'theme': '新西兰电子卡零售销售年率', 'theme_time': '七月', 'importance': '低', 'result': '11.4', 'e_Forecast': '', 'b_value': '8.0', 'result_unit': '%', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '07:01', 'country': '英国', 'theme': '英国零售业联盟(BRC)零售销售监控数据年率', 'theme_time': '七月', 'importance': '中', 'result': '4.3', 'e_Forecast': '', 'b_value': '10.9', 'result_unit': '%', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '07:50', 'country': '日本', 'theme': '日本季调后经常帐', 'theme_time': '', 'importance': '中', 'result': '1.05', 'e_Forecast': '', 'b_value': '0.82', 'result_unit': 'T', 'b_value_unit': 'T', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '07:50', 'country': '日本', 'theme': '日本季调后银行贷款年率', 'theme_time': '七月', 'importance': '低', 'result': '6.3', 'e_Forecast': '', 'b_value': '6.2', 'result_unit': '%', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '07:50', 'country': '日本', 'theme': '日本未季调经常帐(日元)', 'theme_time': '六月', 'importance': '中', 'result': '0.168', 'e_Forecast': '0.110', 'b_value': '1.177', 'result_unit': 'T', 'b_value_unit': 'T', 'e_Forecast_unit': 'T'}, {'real_date': '2020-8-11', 'real_time': '08:00', 'country': '新加坡', 'theme': '新加坡国内生产总值(GDP)季率', 'theme_time': '第三季度', 'importance': '低', 'result': '-42.9', 'e_Forecast': '-42.9', 'b_value': '-3.1', 'result_unit': '%', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '08:00', 'country': '新加坡', 'theme': '新加坡国内生产总值(GDP)年率', 'theme_time': '第三季度', 'importance': '低', 'result': '-13.2', 'e_Forecast': '-13.2', 'b_value': '-12.6', 'result_unit': '%', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '09:30', 'country': '澳大利亚', 'theme': '澳大利亚NAB商业信心指数', 'theme_time': '七月', 'importance': '中', 'result': '-14', 'e_Forecast': '', 'b_value': '1', 'result_unit': '', 'b_value_unit': '', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '09:30', 'country': '澳大利亚', 'theme': '澳大利亚国民银行(NAB)商业景气指数', 'theme_time': '七月', 'importance': '低', 'result': '0', 'e_Forecast': '', 'b_value': '-7', 'result_unit': '', 'b_value_unit': '', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '11:00', 'country': '印度尼西亚', 'theme': '印度尼西亚国际收支平衡(美元)', 'theme_time': '第二季度', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '-8.5', 'result_unit': '', 'b_value_unit': 'B', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '11:00', 'country': '印度尼西亚', 'theme': '印度尼西亚经常帐余额占GDP比重', 'theme_time': '第二季度', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '-1.40', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '13:00', 'country': '日本', 'theme': '日本经济观察家前景指数', 'theme_time': '七月', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '38.8', 'result_unit': '', 'b_value_unit': '', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '16:30', 'country': '英国', 'theme': '英国剔除红利三个月平均工资年率', 'theme_time': '六月', 'importance': '低', 'result': '', 'e_Forecast': '-0.1', 'b_value': '0.7', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '16:30', 'country': '英国', 'theme': '英国含红利三个月平均工资年率', 'theme_time': '六月', 'importance': '高', 'result': '', 'e_Forecast': '-1.1', 'b_value': '-0.3', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '16:30', 'country': '英国', 'theme': '英国失业金申请人数', 'theme_time': '七月', 'importance': '高', 'result': '', 'e_Forecast': '10.0', 'b_value': '-28.1', 'result_unit': '', 'b_value_unit': 'K', 'e_Forecast_unit': 'K'}, {'real_date': '2020-8-11', 'real_time': '16:30', 'country': '英国', 'theme': '英国三个月ILO就业人数变动(人)', 'theme_time': '六月', 'importance': '中', 'result': '', 'e_Forecast': '-288', 'b_value': '-126', 'result_unit': '', 'b_value_unit': 'K', 'e_Forecast_unit': 'K'}, {'real_date': '2020-8-11', 'real_time': '16:30', 'country': '英国', 'theme': '英国失业率', 'theme_time': '六月', 'importance': '中', 'result': '', 'e_Forecast': '4.2', 'b_value': '3.9', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '17:00', 'country': '德国', 'theme': '德国ZEW经济现况指数', 'theme_time': '八月', 'importance': '中', 'result': '', 'e_Forecast': '-68.8', 'b_value': '-80.9', 'result_unit': '', 'b_value_unit': '', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '17:00', 'country': '德国', 'theme': '德国ZEW经济景气指数', 'theme_time': '八月', 'importance': '高', 'result': '', 'e_Forecast': '58.0', 'b_value': '59.3', 'result_unit': '', 'b_value_unit': '', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '17:00', 'country': '西班牙', 'theme': '西班牙12个月期国债拍卖平均收益率', 'theme_time': '', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '-0.463', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '17:00', 'country': '西班牙', 'theme': '西班牙6个月期国债拍卖平均收益率', 'theme_time': '', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '-0.506', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '17:00', 'country': '欧元区', 'theme': '欧元区ZEW经济景气指数', 'theme_time': '八月', 'importance': '中', 'result': '', 'e_Forecast': '', 'b_value': '59.6', 'result_unit': '', 'b_value_unit': '', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '17:30', 'country': '南非', 'theme': '南非失业率', 'theme_time': '第二季度', 'importance': '低', 'result': '', 'e_Forecast': '29.70', 'b_value': '30.10', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '17:30', 'country': '南非', 'theme': '南非失业率', 'theme_time': '第二季度', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '7.100', 'result_unit': '', 'b_value_unit': 'M', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '17:45', 'country': '英国', 'theme': '英国5年期国债拍卖收益率', 'theme_time': '', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '-0.033', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '18:00', 'country': '美国', 'theme': '美国NFIB小型企业信心指数', 'theme_time': '七月', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '100.6', 'result_unit': '', 'b_value_unit': '', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '19:00', 'country': '巴西', 'theme': '巴西央行货币政策委员会会议纪要', 'theme_time': '', 'importance': '高', 'result': '', 'e_Forecast': '', 'b_value': '', 'result_unit': '', 'b_value_unit': '', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '19:00', 'country': '南非', 'theme': '南非制造业产出月率', 'theme_time': '六月', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '-44.3', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '20:00', 'country': '美国', 'theme': '美国EIA短期能源展望', 'theme_time': '', 'importance': '高', 'result': '', 'e_Forecast': '', 'b_value': '', 'result_unit': '', 'b_value_unit': '', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '20:00', 'country': '印度', 'theme': '印度累计工业生产指数年率', 'theme_time': '六月', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '-0.70', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '20:00', 'country': '印度', 'theme': '印度工业生产指数年率', 'theme_time': '六月', 'importance': '低', 'result': '', 'e_Forecast': '-20.0', 'b_value': '-18.3', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '20:00', 'country': '印度', 'theme': '印度制造业产出月率', 'theme_time': '六月', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '-22.4', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '20:15', 'country': '加拿大', 'theme': '加拿大新屋开工年化总数', 'theme_time': '七月', 'importance': '中', 'result': '', 'e_Forecast': '210.0', 'b_value': '211.7', 'result_unit': '', 'b_value_unit': 'K', 'e_Forecast_unit': 'K'}, {'real_date': '2020-8-11', 'real_time': '20:30', 'country': '美国', 'theme': '美国核心PPI月率', 'theme_time': '七月', 'importance': '中', 'result': '', 'e_Forecast': '0.1', 'b_value': '-0.3', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '20:30', 'country': '美国', 'theme': '美国核心生产者物价指数(PPI)年率', 'theme_time': '七月', 'importance': '低', 'result': '', 'e_Forecast': '0.4', 'b_value': '0.1', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '20:30', 'country': '美国', 'theme': '美国生产者物价指数(PPI)年率', 'theme_time': '七月', 'importance': '低', 'result': '', 'e_Forecast': '-0.7', 'b_value': '-0.8', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '20:30', 'country': '美国', 'theme': '美国生产者物价指数(PPI)月率', 'theme_time': '七月', 'importance': '高', 'result': '', 'e_Forecast': '0.3', 'b_value': '-0.2', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '20:30', 'country': '加拿大', 'theme': '加拿大营建许可月率', 'theme_time': '六月', 'importance': '中', 'result': '', 'e_Forecast': '', 'b_value': '20.2', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '20:55', 'country': '美国', 'theme': '美国红皮书商业零售销售月率', 'theme_time': '', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '1.1', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '20:55', 'country': '美国', 'theme': '美国红皮书商业零售销售年率', 'theme_time': '', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '-7.1', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '21:00', 'country': '俄罗斯', 'theme': '俄罗斯贸易帐', 'theme_time': '六月', 'importance': '低', 'result': '', 'e_Forecast': '4.10', 'b_value': '3.67', 'result_unit': '', 'b_value_unit': 'B', 'e_Forecast_unit': 'B'}, {'real_date': '2020-8-11', 'real_time': '', 'country': '英国', 'theme': '英国国家经济社会研究院(NIESR)-GDP预估', 'theme_time': '', 'importance': '中', 'result': '', 'e_Forecast': '', 'b_value': '-21.2', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '22:00', 'country': '墨西哥', 'theme': '墨西哥工业产出月率', 'theme_time': '六月', 'importance': '低', 'result': '', 'e_Forecast': '17.0', 'b_value': '-1.8', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '22:00', 'country': '墨西哥', 'theme': '墨西哥工业生产指数年率', 'theme_time': '六月', 'importance': '低', 'result': '', 'e_Forecast': '-17.8', 'b_value': '-30.7', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}, {'real_date': '2020-8-11', 'real_time': '23:05', 'country': '印度尼西亚', 'theme': '印度尼西亚零售销售年率', 'theme_time': '六月', 'importance': '低', 'result': '', 'e_Forecast': '', 'b_value': '-20.6', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': ''}, {'real_date': '2020-8-11', 'real_time': '23:05', 'country': '俄罗斯', 'theme': '俄罗斯月度国内生产总值(GDP)年率', 'theme_time': '第二季度', 'importance': '中', 'result': '', 'e_Forecast': '-9.0', 'b_value': '1.6', 'result_unit': '', 'b_value_unit': '%', 'e_Forecast_unit': '%'}]
    # send_1999data(list_1)
