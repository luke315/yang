# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import time
import pymysql

from API.get_data_api import get_data_T1, get_data_week, get_company_theme
from dailyfx.spiders.inversting_com_T import delete_set
from lib.delete_log import delete_logs_star
from lib.lib_args import the_version, get_week_day, get_theme_type, current_time
from lib.mysql_setting import host, port, user, password, db

class DailyfxPipeline1(object):
    conn = None
    cursor = None

    def open_spider(self, spider):
        self.conn = pymysql.Connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db)

    def close_spider(self, spider):
        self.conn.close()
        # 发送预测数据
        if spider.name == 'dailyfx_after30':
            get_data_T1('dailyfx')
            week = get_week_day(datetime.datetime.now())
            if week == '星期五':
                get_data_week('dailyfx')

    def process_item(self, item, spider):
        self.conn.ping(reconnect=True)

        if spider.name == 'dailyfx_after30':
            the_v = the_version
        else:
            the_v = '1'
        self.cursor = self.conn.cursor()
        select_sql = 'SELECT * FROM event_calendar WHERE date_theme = %s AND the_version = %s'
        self.cursor.execute(select_sql, (pymysql.escape_string(item['real_date'] + item['theme']), the_v))
        result = self.cursor.fetchone()
        # print(result)
        if result is None:
            # 插入
            sql = 'insert into event_calendar (date_theme, real_date, real_time, country, theme, importance, result, result_unit, e_Forecast, e_Forecast_unit, b_value, b_value_unit, explan ,the_version) values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' \
                  % (pymysql.escape_string(item['real_date'] + item['theme']), item['real_date'],
                     item['real_time'],
                     item['country'], pymysql.escape_string(item['theme']), item['importance'],
                     item['result'], item['result_unit'], item['e_Forecast'], item['e_Forecast_unit'],
                     item['b_value'],
                     item['b_value_unit'], item['explan'], the_v)
            try:
                self.cursor.execute(sql)
                self.conn.commit()
            except Exception as e:
                print(e)
                self.conn.rollback()
        else:
            sql = 'update event_calendar set date_theme = %s, real_date = %s, real_time = %s, country = %s, theme = %s, result = %s, result_unit = %s, e_Forecast = %s, e_Forecast_unit = %s, b_value = %s, b_value_unit = %s, explan =%s where date_theme =%s AND the_version = %s'
            try:
                self.cursor.execute(sql,
                                    (pymysql.escape_string(item['real_date'] + item['theme']), item['real_date'],
                                     item['real_time'],
                                     item['country'], pymysql.escape_string(item['theme']),
                                     item['result'], item['result_unit'], item['e_Forecast'],
                                     item['e_Forecast_unit'], item['b_value'], item['b_value_unit'],
                                     item['explan'], pymysql.escape_string(item['real_date'] + item['theme']),
                                     the_v))
                self.conn.commit()
            except Exception as e:
                print(e)
                self.conn.rollback()
        return item

class Investing_Item_Pipeline(object):
    conn = None
    cursor = None

    def open_spider(self, spider):
        # 链接数据库
        self.conn = pymysql.Connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db)

    def close_spider(self, spider):
        # 发送预测数据
        if spider.name == 'investing_com_F':
            get_data_T1('investing')
            week = get_week_day(datetime.datetime.now())
            if week == '星期五':
                get_data_week('investing')
                # 删除日志
                delete_logs_star()
        # else:
        #     # 删除未更新事件
        #     self.delete_theme()
        self.conn.close()

    def process_item(self, item, spider):

        self.conn.ping(reconnect=True)
        # 预测数据
        if spider.name == 'investing_com_F':
            the_v = the_version
            # 判断是否为叮叮历史事件
            res = get_company_theme('dingding', item['theme'])
            # 讲话等事件
            res1 = get_theme_type(item['theme'])
            if res or res1 or item['real_time'] == '':
                api = 'dingding'
            else:
                api = ''
        else:
            api = ''
            the_v = '1'

        if item['real_time'] == '':
            item['real_time'] = '00:00'

        self.cursor = self.conn.cursor()
        select_sql = 'SELECT * FROM Investing WHERE theme=%s AND real_date=%s AND the_version=%s'
        self.cursor.execute(select_sql,
                            (pymysql.escape_string(item['theme']), item['real_date'], the_v))
        result = self.cursor.fetchone()
        # print(result)
        if result is None:
            # 插入
            sql = 'insert into Investing (real_date, real_time, country, theme, theme_time, result, result_unit, importance, e_Forecast, e_Forecast_unit, b_value, b_value_unit, type_theme, the_version, api) values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' \
                  % (item['real_date'], item['real_time'], item['country'], pymysql.escape_string(item['theme']),
                     item['theme_time'], item['result'], item['result_unit'], item['importance'],
                     item['e_Forecast'], item['e_Forecast_unit'], item['b_value'], item['b_value_unit'],
                     item['type_theme'], the_v, api)
            try:
                self.cursor.execute(sql)
                self.conn.commit()
            except Exception as e:
                print(e, item)
                self.conn.rollback()
        else:
            sql = 'update Investing set importance=%s, result = %s, result_unit = %s, e_Forecast = %s, e_Forecast_unit = %s, b_value = %s, b_value_unit = %s, country=%s where theme=%s AND real_date=%s AND the_version=%s'
            try:
                self.cursor.execute(sql, (
                item['importance'], item['result'], item['result_unit'], item['e_Forecast'],
                item['e_Forecast_unit'], item['b_value'], item['b_value_unit'], item['country'],
                pymysql.escape_string(item['theme']), item['real_date'], the_v))
                self.conn.commit()
            except Exception as e:
                print(e)
                self.conn.rollback()

        # 更新预测数据
        self.update_F(item)
        return item

    # 更新预测数据
    def update_F(self, item):
        e_time = datetime.timedelta(days=1)
        the_v = current_time - e_time

        self.cursor = self.conn.cursor()
        select_sql = 'SELECT * FROM Investing WHERE theme=%s AND real_date=%s AND the_version=%s'
        self.cursor.execute(select_sql,
                            (pymysql.escape_string(item['theme']), item['real_date'], the_v))
        result = self.cursor.fetchone()
        if result != None:
            sql = 'update Investing set importance=%s, result = %s, result_unit = %s, e_Forecast = %s, e_Forecast_unit = %s, b_value = %s, b_value_unit = %s, country=%s where theme=%s AND real_date=%s AND the_version=%s'
            try:
                self.cursor.execute(sql, (
                    item['importance'], item['result'], item['result_unit'], item['e_Forecast'],
                    item['e_Forecast_unit'], item['b_value'], item['b_value_unit'], item['country'],
                    pymysql.escape_string(item['theme']), item['real_date'], the_v))
                self.conn.commit()
            except Exception as e:
                print(e)
                self.conn.rollback()

    # def delete_theme(self):
    #     list_theme = []
    #     # 今天捕获事件的列表
    #     list_data = list(delete_set)
    #     cursor = self.conn.cursor()
    #     select_sql = 'SELECT theme FROM Investing WHERE real_date=%s AND the_version=%s'
    #     cursor.execute(select_sql, (str(current_time), '1'))
    #     result = cursor.fetchall()
    #     for theme in result:
    #         theme = theme[0]
    #         if theme not in list_data:
    #             list_theme.append(theme)
    #             # print('not in', theme)
    #             delete_sql_T = """delete from investing where theme=%s AND real_date=%s AND the_version=%s"""
    #             self.cursor.execute(delete_sql_T, (theme,str(current_time), '1'))
    #             # 预测版本T-1
    #             F_ver = current_time - datetime.timedelta(days=1)
    #             delete_sql_F = """delete from investing where theme=%s AND real_date=%s AND the_version=%s"""
    #             self.cursor.execute(delete_sql_F, (theme, str(current_time), F_ver))
    #         else:
    #             # print('in', theme)
    #             pass
    #     delete_theme(list_theme)
