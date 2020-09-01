# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import datetime

import pymysql
from lib.db_args import host, port, user, password, db, database_info_target, database_info_target2, \
    database_info_target_sit, database_info_target_sit_datacenter1, database_info_target_pro_datacenter1, \
    database_info_target_pro
from lib.lib_args import mysql_setting
from lib.md5 import md5_sql
from .items import Eia_Basic_Item, Eia_Epx_Item
import psycopg2
from twisted.enterprise import adbapi
from pymysql import cursors


class mysqlTwistedpipline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(
            host=host,
            port=port,
            db=db,
            user=user,
            passwd=password,
            charset='utf8',
            cursorclass=cursors.DictCursor,
            use_unicode=True
        )
        dbpool = adbapi.ConnectionPool('pymysql', **dbparms)
        return cls(dbpool)

    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item)
        return item

    def handle_error(self, failure, item):
        # print(failure, 'item:%s' % item)  #
        pass

    def do_insert(self, cursor, item):
        self.dbpool.ping(reconnect=True)
        if isinstance(item, Eia_Epx_Item):
            if item['epx_code'] == 'leaf_page':
                ext4 = 'LEAF'
            else:
                ext4 = 'NODE'
            id = item['epx_code']
            praent_id = item['epx_parent_code']
            epx_url = {'id': id, 'praent_id': praent_id}

            # 转义
            # epx_url = pymysql.escape_string(epx_url)

            # 加密处理目录
            epx_code = md5_sql(item['epx_code'])
            epx_parent_code = md5_sql(item['epx_parent_code'])
            # 插入
            sql = """insert into t_exponent_eia (uid, code, parent_code, exponent_name, epx_url, epx_units, epx_frequency, NODE_LEAF) values('%s','%s','%s','%s','%s','%s','%s','%s')""" \
                  % (md5_sql(item['epx_parent_code'] + item['epx_parent_code']), epx_code, epx_parent_code,
                     item['epx_name'], epx_url, item['epx_units'], item['epx_frequency'], ext4)
            cursor.execute(sql)
        elif isinstance(item, Eia_Basic_Item):
            epx_code = md5_sql(item['epx_code'])
            epx_parent_code = md5_sql(item['epx_parent_code'])
            # 插入
            sql = """insert into t_exponent_value119 (uid, code, period, epx_value) values('%s','%s','%s','%s')""" \
                  % (md5_sql(item['epx_code'] + item['period']), epx_code, item['period'], item['epx_value'])
            cursor.execute(sql)
        return item


# eia_table_name = 't_exponent_eia_copy1'
# eia_value_table_name = 't_exponent_value_eia_copy1'

eia_table_name = 't_exponent_eia'
eia_value_table_name = 't_exponent_value_eia'


class Eia_Postgre_Pipeline:
    conn = None
    cursor = None

    # def open_spider(self, spider):
    #     pass

    # def close_spider(self, spider):
    #     self.conn.close()

    def process_item(self, item, spider):
        if isinstance(item, Eia_Epx_Item):
            self.conn = psycopg2.connect(
                database=database_info_target['database'],
                user=database_info_target['user'],
                password=database_info_target['password'],
                host=database_info_target['host'],
                port=database_info_target['port'])

            self.cursor = self.conn.cursor()

            if item['epx_frequency'] != '':
                ext4 = 'LEAF'
            else:
                ext4 = 'NODE'
            # 加密处理目录
            epx_code = md5_sql(item['epx_code'])
            epx_parent_code = md5_sql(item['epx_parent_code'])
            # 查看
            select_sql = f"""SELECT * FROM {eia_table_name} WHERE code = %s AND parent_code = %s"""
            self.cursor.execute(select_sql, (epx_code, epx_parent_code))
            result = self.cursor.fetchone()
            # print(result)
            if result is None:
                # 插入
                epx_url = str({'id': item['epx_code'], 'praent_id': item['epx_parent_code']}).replace("'", "''")
                sql = f"""insert into {eia_table_name}(code, parent_code, desc_s, status, ext1, ext2, ext3, ext4) values('%s','%s','%s','%s','%s','%s','%s','%s')""" \
                      % (epx_code, epx_parent_code, item['epx_name'].replace('\"', '').replace('\'', ''),
                          'valid', item['epx_units'], epx_url, item['epx_frequency'], ext4)
                try:
                    self.cursor.execute(sql)
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    self.conn.rollback()
            else:
                epx_url = str({'id': item['epx_code'], 'praent_id': item['epx_parent_code']})
                sql = f"""update {eia_table_name} set desc_s=%s, ext1=%s, ext2=%s, ext3=%s, ext4=%s WHERE code = %s AND parent_code = %s"""
                try:
                    self.cursor.execute(sql, (
                    item['epx_name'].replace('\"', '').replace('\'', ''), item['epx_units'], epx_url,
                    item['epx_frequency'], ext4, epx_code, epx_parent_code))
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    self.conn.rollback()
        elif isinstance(item, Eia_Basic_Item):
            self.conn = psycopg2.connect(
                database=database_info_target2['database'],
                user=database_info_target2['user'],
                password=database_info_target2['password'],
                host=database_info_target2['host'],
                port=database_info_target2['port'])
            self.cursor = self.conn.cursor()
            epx_code = md5_sql(item['epx_code'])
            # 插入 the_year, the_season, the_month, the_date
            if item['epx_value'] == 'null':
                item['epx_value'] = ''
            if item['frequency'] == 'YEAR':
                # select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_year=%s"""
                # self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), item['period']))
                # result = self.cursor.fetchone()
                # print(result)

                select_sql = f"""SELECT exponent_value FROM {eia_value_table_name} WHERE exponent_code=%s and the_year=%s"""
                self.cursor.execute(select_sql, (epx_code, item['period']))
                result = self.cursor.fetchone()
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_year) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], item['period'])
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                    # 更新exp表
                    eia_exponent_update_time(epx_code, '123')
                else:
                    # 如果值相等，则不管
                    if result[0] == item['epx_value']:
                        pass
                    else:
                        sql = f"""update {eia_value_table_name} set exponent_value=%s, update_time=%s where exponent_code=%s and the_year=%s"""
                        try:
                            self.cursor.execute(sql, (item['epx_value'], str(datetime.datetime.now()), epx_code, item['period']))
                            self.conn.commit()
                        except Exception as e:
                            print(e)
                            self.conn.rollback()
                        # 更新exp表
                        eia_exponent_update_time(epx_code, '123')
            elif item['frequency'] == 'SEASON':
                i = item['period']
                year = i[0:4]
                q = i[4] + i[5]
                if q == 'Q1':
                    q = '03'
                elif q == 'Q2':
                    q = '06'
                elif q == 'Q3':
                    q = '09'
                elif q == 'Q4':
                    q = '12'
                season = year + q

                # select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_season=%s"""
                # self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), season))

                select_sql = f"""SELECT exponent_value FROM {eia_value_table_name} WHERE exponent_code=%s and the_season=%s"""
                self.cursor.execute(select_sql, (epx_code, season))

                result = self.cursor.fetchone()
                # print(result)
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_season) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], season)
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                    # 更新exp表
                    eia_exponent_update_time(epx_code, '123')
                else:
                    # 如果值相等，则不管
                    if result[0] == item['epx_value']:
                        pass
                    else:
                        sql = f"""update {eia_value_table_name} set exponent_value=%s, update_time=%s where exponent_code=%s and the_season=%s"""
                        try:
                            self.cursor.execute(sql, (item['epx_value'], str(datetime.datetime.now()), epx_code, season))
                            self.conn.commit()
                        except Exception as e:
                            print(e)
                            self.conn.rollback()
                        # 更新exp表
                        eia_exponent_update_time(epx_code, '123')
            elif item['frequency'] == 'MONTH':
                # select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_month=%s"""
                # self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), item['period']))

                select_sql = f"""SELECT exponent_value FROM {eia_value_table_name} WHERE exponent_code=%s and the_month=%s"""
                self.cursor.execute(select_sql, (epx_code, item['period']))
                result = self.cursor.fetchone()
                # print(result)
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_month) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], item['period'])
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                    # 更新exp表
                    eia_exponent_update_time(epx_code, '123')
                else:
                    # 如果值相等，则不管
                    if result[0] == item['epx_value']:
                        pass
                    else:
                        sql = f"""update {eia_value_table_name} set exponent_value=%s, update_time=%s where exponent_code=%s and the_month=%s"""
                        try:
                            self.cursor.execute(sql, (item['epx_value'], str(datetime.datetime.now()), epx_code, item['period']))
                            self.conn.commit()
                        except Exception as e:
                            print(e)
                            self.conn.rollback()
                        # 更新exp表
                        eia_exponent_update_time(epx_code, '123')
            else:
                # 如果是小时，period不处理
                if item['frequency'] == 'HOUR':
                    date = item['period']
                else:
                    # 频率为周和天，处理成2020-05-20类似格式
                    i = item['period']
                    date = f'{i[0:4]}-{i[4:6]}-{i[6] + i[7]}'
                # select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_date=%s"""
                # self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), date))

                select_sql = f"""SELECT exponent_value FROM {eia_value_table_name} WHERE exponent_code=%s and the_date=%s"""
                self.cursor.execute(select_sql, (epx_code, date))
                result = self.cursor.fetchone()
                # print(result)
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_date) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], date)
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                    # 更新exp表
                    eia_exponent_update_time(epx_code, '123')
                else:
                    # 如果值相等，则不管
                    if result[0] == item['epx_value']:
                        pass
                    else:
                        sql = f"""update {eia_value_table_name} set exponent_value=%s, update_time=%s where exponent_code=%s and the_date=%s"""
                        try:
                            self.cursor.execute(sql, (item['epx_value'], str(datetime.datetime.now()), epx_code, date))
                            self.conn.commit()
                        except Exception as e:
                            print(e)
                            self.conn.rollback()
                        # 更新exp表
                        eia_exponent_update_time(epx_code, '123')
        self.conn.close()
        return item


class Eia_Postgre_sit_Pipeline:
    conn = None
    cursor = None

    # def open_spider(self, spider):
    #     pass

    # def close_spider(self, spider):
    #     self.conn.close()

    def process_item(self, item, spider):
        if isinstance(item, Eia_Epx_Item):
            self.conn = psycopg2.connect(
                database=database_info_target_sit['database'],
                user=database_info_target_sit['user'],
                password=database_info_target_sit['password'],
                host=database_info_target_sit['host'],
                port=database_info_target_sit['port'])
            self.cursor = self.conn.cursor()
            if item['epx_frequency'] != '':
                ext4 = 'LEAF'
            else:
                ext4 = 'NODE'
            # 加密处理目录
            epx_code = md5_sql(item['epx_code'])
            epx_parent_code = md5_sql(item['epx_parent_code'])
            # 查看
            select_sql = f"""SELECT * FROM {eia_table_name} WHERE code = %s AND parent_code = %s"""
            self.cursor.execute(select_sql, (epx_code, epx_parent_code))
            result = self.cursor.fetchone()
            # print(result)
            if result is None:
                # 插入
                epx_url = str({'id': item['epx_code'], 'praent_id': item['epx_parent_code']}).replace("'", "''")
                sql = f"""insert into {eia_table_name}(code, parent_code, desc_s, status, ext1, ext2, ext3, ext4) values('%s','%s','%s','%s','%s','%s','%s','%s')""" \
                      % (epx_code, epx_parent_code, item['epx_name'].replace('\"', '').replace('\'', ''),
                         'valid', item['epx_units'], epx_url, item['epx_frequency'], ext4)
                try:
                    self.cursor.execute(sql)
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    self.conn.rollback()
            else:
                epx_url = str({'id': item['epx_code'], 'praent_id': item['epx_parent_code']})
                sql = f"""update {eia_table_name} set desc_s=%s, ext1=%s, ext2=%s, ext3=%s, ext4=%s WHERE code = %s AND parent_code = %s"""
                try:
                    self.cursor.execute(sql, (
                    item['epx_name'].replace('\"', '').replace('\'', ''), item['epx_units'], epx_url,
                    item['epx_frequency'], ext4, epx_code, epx_parent_code))
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    self.conn.rollback()
        elif isinstance(item, Eia_Basic_Item):
            self.conn = psycopg2.connect(
                database=database_info_target_sit_datacenter1['database'],
                user=database_info_target_sit_datacenter1['user'],
                password=database_info_target_sit_datacenter1['password'],
                host=database_info_target_sit_datacenter1['host'],
                port=database_info_target_sit_datacenter1['port'])
            self.cursor = self.conn.cursor()
            epx_code = md5_sql(item['epx_code'])
            # 插入 the_year, the_season, the_month, the_date
            if item['epx_value'] == 'null':
                item['epx_value'] = ''
            if item['frequency'] == 'YEAR':
                select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_year=%s"""
                self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), item['period']))
                result = self.cursor.fetchone()
                # print(result)
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_year) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], item['period'])
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                else:
                    sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_year=%s"""
                    try:
                        self.cursor.execute(sql, (item['epx_value'], epx_code, item['period']))
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
            elif item['frequency'] == 'SEASON':
                i = item['period']
                year = i[0:4]
                q = i[4] + i[5]
                if q == 'Q1':
                    q = '03'
                elif q == 'Q2':
                    q = '06'
                elif q == 'Q3':
                    q = '09'
                elif q == 'Q4':
                    q = '12'
                season = year + q

                select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_season=%s"""
                self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), season))
                result = self.cursor.fetchone()
                # print(result)
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_season) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], season)
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                else:
                    sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_season=%s"""
                    try:
                        self.cursor.execute(sql, (item['epx_value'], epx_code, season))
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
            elif item['frequency'] == 'MONTH':
                select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_month=%s"""
                self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), item['period']))
                result = self.cursor.fetchone()
                # print(result)
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_month) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], item['period'])
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                else:
                    sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_month=%s"""
                    try:
                        self.cursor.execute(sql, (item['epx_value'], epx_code, item['period']))
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
            else:
                # 如果是小时，period不处理
                if item['frequency'] == 'HOUR':
                    date = item['period']
                else:
                    # 频率为周和天，处理成2020-05-20类似格式
                    i = item['period']
                    date = f'{i[0:4]}-{i[4:6]}-{i[6] + i[7]}'
                select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_date=%s"""
                self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), date))
                result = self.cursor.fetchone()
                # print(result)
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_date) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], date)
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                else:
                    sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_date=%s"""
                    try:
                        self.cursor.execute(sql, (item['epx_value'], epx_code, date))
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
            eia_exponent_update_time(epx_code, 'sit')
        self.conn.close()
        return item


class Eia_Postgre_pro_Pipeline:
    conn = None
    cursor = None

    # def open_spider(self, spider):
    #     pass

    # def close_spider(self, spider):
    #     self.conn.close()

    def process_item(self, item, spider):
        if isinstance(item, Eia_Epx_Item):
            self.conn = psycopg2.connect(
                database=database_info_target_pro['database'],
                user=database_info_target_pro['user'],
                password=database_info_target_pro['password'],
                host=database_info_target_pro['host'],
                port=database_info_target_pro['port'])

            self.cursor = self.conn.cursor()

            if item['epx_frequency'] != '':
                ext4 = 'LEAF'
            else:
                ext4 = 'NODE'
            # 加密处理目录
            epx_code = md5_sql(item['epx_code'])
            epx_parent_code = md5_sql(item['epx_parent_code'])
            # 查看
            select_sql = f"""SELECT * FROM {eia_table_name} WHERE code = %s AND parent_code = %s"""
            self.cursor.execute(select_sql, (epx_code, epx_parent_code))
            result = self.cursor.fetchone()
            # print(result)
            if result is None:
                # 插入
                epx_url = str({'id': item['epx_code'], 'praent_id': item['epx_parent_code']}).replace("'", "''")
                sql = f"""insert into {eia_table_name}(code, parent_code, desc_s, status, ext1, ext2, ext3, ext4) values('%s','%s','%s','%s','%s','%s','%s','%s')""" \
                      % (epx_code, epx_parent_code, item['epx_name'].replace('\"', '').replace('\'', ''),
                         'valid', item['epx_units'], epx_url, item['epx_frequency'], ext4)
                try:
                    self.cursor.execute(sql)
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    self.conn.rollback()
            else:
                epx_url = str({'id': item['epx_code'], 'praent_id': item['epx_parent_code']})
                sql = f"""update {eia_table_name} set desc_s=%s, ext1=%s, ext2=%s, ext3=%s, ext4=%s WHERE code = %s AND parent_code = %s"""
                try:
                    self.cursor.execute(sql, (
                    item['epx_name'].replace('\"', '').replace('\'', ''), item['epx_units'], epx_url,
                    item['epx_frequency'], ext4, epx_code, epx_parent_code))
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    self.conn.rollback()
        elif isinstance(item, Eia_Basic_Item):
            self.conn = psycopg2.connect(
                database=database_info_target_pro_datacenter1['database'],
                user=database_info_target_pro_datacenter1['user'],
                password=database_info_target_pro_datacenter1['password'],
                host=database_info_target_pro_datacenter1['host'],
                port=database_info_target_pro_datacenter1['port'])
            self.cursor = self.conn.cursor()
            epx_code = md5_sql(item['epx_code'])
            # 插入 the_year, the_season, the_month, the_date
            if item['epx_value'] == 'null':
                item['epx_value'] = ''
            if item['frequency'] == 'YEAR':
                select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_year=%s"""
                self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), item['period']))
                result = self.cursor.fetchone()
                # print(result)
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_year) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], item['period'])
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                else:
                    sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_year=%s"""
                    try:
                        self.cursor.execute(sql, (item['epx_value'], epx_code, item['period']))
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
            elif item['frequency'] == 'SEASON':
                i = item['period']
                year = i[0:4]
                q = i[4] + i[5]
                if q == 'Q1':
                    q = '03'
                elif q == 'Q2':
                    q = '06'
                elif q == 'Q3':
                    q = '09'
                elif q == 'Q4':
                    q = '12'
                season = year + q

                select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_season=%s"""
                self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), season))
                result = self.cursor.fetchone()
                # print(result)
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_season) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], season)
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                else:
                    sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_season=%s"""
                    try:
                        self.cursor.execute(sql, (item['epx_value'], epx_code, season))
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
            elif item['frequency'] == 'MONTH':
                select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_month=%s"""
                self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), item['period']))
                result = self.cursor.fetchone()
                # print(result)
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_month) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], item['period'])
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                else:
                    sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_month=%s"""
                    try:
                        self.cursor.execute(sql, (item['epx_value'], epx_code, item['period']))
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
            else:
                # 如果是小时，period不处理
                if item['frequency'] == 'HOUR':
                    date = item['period']
                else:
                    # 频率为周和天，处理成2020-05-20类似格式
                    i = item['period']
                    date = f'{i[0:4]}-{i[4:6]}-{i[6] + i[7]}'
                select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_date=%s"""
                self.cursor.execute(select_sql, (epx_code, str(item['epx_value']), date))
                result = self.cursor.fetchone()
                # print(result)
                if result is None:
                    sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_date) values('%s','%s','%s')""" \
                          % (epx_code, item['epx_value'], date)
                    try:
                        self.cursor.execute(sql)
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                else:
                    sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_date=%s"""
                    try:
                        self.cursor.execute(sql, (item['epx_value'], epx_code, date))
                        self.conn.commit()
                    except Exception as e:
                        print(e)
                        self.conn.rollback()
                eia_exponent_update_time(epx_code, 'pro')
        self.conn.close()
        return item


def eia_exponent_update_time(exponent_code, db):
    if db == '123':
        conn = psycopg2.connect(
            database=database_info_target["database"],
            user=database_info_target["user"],
            password=database_info_target["password"],
            host=database_info_target["host"],
            port=database_info_target["port"]
        )
    elif db == 'sit':
        conn = psycopg2.connect(
            database=database_info_target_sit["database"],
            user=database_info_target_sit["user"],
            password=database_info_target_sit["password"],
            host=database_info_target_sit["host"],
            port=database_info_target_sit["port"]
        )
    else:
        conn = psycopg2.connect(
            database=database_info_target_pro["database"],
            user=database_info_target_pro["user"],
            password=database_info_target_pro["password"],
            host=database_info_target_pro["host"],
            port=database_info_target_pro["port"]
        )
    cursor = conn.cursor()
    sql = f"""update t_exponent_eia set update_time=%s WHERE code=%s AND status=%s"""
    try:
        cursor.execute(sql, (str(datetime.datetime.now()), exponent_code, 'valid'))
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()