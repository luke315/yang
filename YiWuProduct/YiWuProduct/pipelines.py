# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import logging

import psycopg2

from YiWuProduct.items import YiWuItem, ValueItem
from lib.db_args import database_info_target, database_info_target2, database_info_target_sit, database_info_target_pro, \
    database_info_target_232
from lib.delete_log import delete_logs_star


# 哈希
import hashlib


def md5_sql(data, str_type):
    data_value = f'{data}us_and_hk{str_type}'
    res_value = hashlib.md5(data_value.encode(encoding='UTF-8')).hexdigest()
    return res_value


class YiwuproductPipeline:
    def process_item(self, item, spider):
        return item


class Yiwu_Postgre_Pipeline:

    def close_spider(self, spider):
        print('close_spider')
        delete_logs_star()

    def process_item(self, item, spider):
        if isinstance(item, YiWuItem):
            conn = psycopg2.connect(
                database=database_info_target['database'],
                user=database_info_target['user'],
                password=database_info_target['password'],
                host=database_info_target['host'],
                port=database_info_target['port'])

            table = 't_exponent_yiwu'
            cursor = conn.cursor()

            epx_code = item['code']
            epx_parent_code = item['parent_code']
            epx_name = item['name']
            epx_units = ''
            epx_frequency = item['frequency']
            if item['frequency'] != '':
                ext4 = 'LEAF'
            else:
                ext4 = 'NODE'
            # 查看
            select_sql = f"""SELECT * FROM {table} WHERE code = %s AND parent_code = %s"""
            cursor.execute(select_sql, (epx_code, epx_parent_code))
            result = cursor.fetchone()
            # print(result)
            epx_url = ''
            if result is None:
                # 插入
                sql = f"""insert into {table}(code, parent_code, exponent_name, desc_s, status, ext1, ext2, ext3, ext4) values('%s','%s','%s','%s','%s','%s','%s','%s','%s')""" \
                      % (epx_code, epx_parent_code, epx_name, epx_name, 'valid', epx_units, epx_url, epx_frequency, ext4)
                try:
                    cursor.execute(sql)
                    conn.commit()
                except Exception as e:
                    print(e, epx_code, epx_parent_code)
                    conn.rollback()
            else:
                sql = f"""update {table} set exponent_name=%s, desc_s=%s, ext1=%s, ext2=%s, ext3=%s, ext4=%s WHERE code = %s AND parent_code = %s"""
                try:
                    cursor.execute(sql, (
                        epx_name, epx_name, epx_units, epx_url, epx_frequency, ext4, epx_code, epx_parent_code))
                    conn.commit()
                except Exception as e:
                    print(e)
                    conn.rollback()
            conn.close()

            # # 更新root表
            # if spider.name == 'yiwu_info_add':
            #     yiwu_classify_main(item, 'yiwu')
        elif isinstance(item, ValueItem):
            table = 't_exponent_value_yiwu'
            conn = psycopg2.connect(
                database=database_info_target2['database'],
                user=database_info_target2['user'],
                password=database_info_target2['password'],
                host=database_info_target2['host'],
                port=database_info_target2['port'])

            exponent_code = item['code']
            exponent_value = item['value']
            date_type_value = item['the_date']

            # 插入 the_year, the_season, the_month, the_date
            if item['frequency'] == 'YEAR':
                date_type = 'the_year'
            elif item['frequency'] == 'SEASON':
                date_type = 'the_season'
            elif item['frequency'] == 'MONTH':
                date_type = 'the_month'
                date_type_value = item['the_date'].split('-')[0] + item['the_date'].split('-')[1]

            elif item['frequency'] == 'DATE' or item['frequency'] == 'WEEK':
                date_type = 'the_date'
            else:
                print(f"异常频率{item['frequency']}")
                return
            # 拼接字段
            fields = 'exponent_value'
            where_fields = f"""exponent_code='{exponent_code}' and {date_type}='{date_type_value}'"""
            result_select = select_sql_func(conn, table, fields, where_fields)
            # 判断值是否存在
            if result_select == []:
                # 不存在则插入
                fields = f"""'{exponent_code}','{exponent_value}','{date_type_value}'"""
                where_fields = f"""exponent_code, exponent_value, {date_type}"""
                insert_sql_func(conn, table, fields, where_fields)
                exponent_update_time(exponent_code, '123', 't_exponent_yiwu')
            else:
                # 值相同则忽略
                if result_select[0] != exponent_value:
                    # 存在即更新
                    fields = f""" exponent_value='{exponent_value}', update_time='{str(datetime.datetime.now())}'"""
                    where_fields = f"""exponent_code='{exponent_code}' and {date_type}='{date_type_value}'"""
                    update_sql_func(conn, table, fields, where_fields)
                    # 更新exp表
                    exponent_update_time(exponent_code, '123', 't_exponent_yiwu')
            conn.close()
        return item

def exponent_update_time(exponent_code, db, table):
    if db == '123':
        conn = psycopg2.connect(
            database=database_info_target["database"],
            user=database_info_target["user"],
            password=database_info_target["password"],
            host=database_info_target["host"],
            port=database_info_target["port"]
        )
    # elif db == 'sit':
    #     conn = psycopg2.connect(
    #         database=database_info_target_sit["database"],
    #         user=database_info_target_sit["user"],
    #         password=database_info_target_sit["password"],
    #         host=database_info_target_sit["host"],
    #         port=database_info_target_sit["port"]
    #     )
    # elif db == 'pro':
    #     conn = psycopg2.connect(
    #         database=database_info_target_pro["database"],
    #         user=database_info_target_pro["user"],
    #         password=database_info_target_pro["password"],
    #         host=database_info_target_pro["host"],
    #         port=database_info_target_pro["port"]
    #     )
    else:
        return
    cursor = conn.cursor()
    sql = f"""update {table} set update_time=%s WHERE code=%s AND status=%s"""
    try:
        cursor.execute(sql, (str(datetime.datetime.now()), exponent_code, 'valid'))
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    conn.close()

def select_sql_func(conn_db, table, fields, where_fields):
    cursor = conn_db.cursor()
    sql = f"""select {fields} from {table} where {where_fields}"""
    cursor.execute(sql)
    result1 = cursor.fetchall()
    return result1

def insert_sql_func(conn_db, table, fields, where_fields):
    cursor = conn_db.cursor()
    sql = f"""insert into {table} ({where_fields}) values({fields})"""
    try:
        cursor.execute(sql)
        conn_db.commit()
    except Exception as e:
        print(e)
        conn_db.rollback()

def update_sql_func(conn_db, table, fields, where_fields):
    cursor = conn_db.cursor()
    sql = f"""update {table} set {fields} where {where_fields}"""

    try:
        cursor.execute(sql)
        conn_db.commit()
    except Exception as e:
        print(e)
        conn_db.rollback()



def yiwu_classify_main(item, args_type):
    if args_type == 'yiwu':
        yiwu_classify('232', item)
        yiwu_classify('sit', item)
        yiwu_classify('pro', item)
    else:
        print(f'未知的{args_type}')


def yiwu_classify(db, item):
    if db == '232':
        conn = psycopg2.connect(
            database=database_info_target_232["database"],
            user=database_info_target_232["user"],
            password=database_info_target_232["password"],
            host=database_info_target_232["host"],
            port=database_info_target_232["port"]
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

    # # 中国数据
    # top = 'ae7ead52-5bc4-11ea-a22c-00d8612d14f2'
    # 热门数据
    top = '971cda0a-5c36-11ea-8a68-00d8612d14f2'

    table = 't_exponent_yiwu'
    str_type = '义乌商品指数'

    if item['code'] == 'start':
        # 根级
        save_root(conn, md5_sql('start', str_type), top, '义乌小商品指数', table, item['code'])
        return

    # 子级
    # save_root(conn, md5_sql(item['code'], str_type), md5_sql(item['parent_code'], str_type),
    #           item['name'], table, item['code'])


# 分类写入root表
def save_root(conn, code, parent_code, exponent_name, ref_table, ref_exponent_code):
    table = 't_exponent_root'
    if ref_table == 't_exponent_yiwu':
        ext1 = '义乌商品指数'
    else:
        logging.error(f'未知{ref_table}')
        return
    cursor = conn.cursor()
    exponent_name = exponent_name.replace("'", "")
    select_sql = f"""select * from {table} where code=%s and parent_code=%s"""
    cursor.execute(select_sql, (code, parent_code))
    result = cursor.fetchone()
    if result is None:
        # 插入
        insert_sql = f"""insert into {table} (code, parent_code, exponent_name, ref_table, ref_exponent_code, ext1, status) values('%s','%s','%s','%s','%s','%s','%s')""" \
                     % (
                         code, parent_code, exponent_name, ref_table,
                         ref_exponent_code, ext1, 'valid')
        try:
            cursor.execute(insert_sql)
            conn.commit()
        except Exception as e:
            print(e, code, parent_code)
            conn.rollback()
    else:
        update_sql = f"""update {table} set exponent_name=%s, ref_table=%s, ref_exponent_code=%s, ext1=%s, status=%s, update_time=%s where code=%s and parent_code=%s"""
        try:
            cursor.execute(update_sql, (
            exponent_name, ref_table, ref_exponent_code, ext1, 'valid',
            str(datetime.datetime.now()), code, parent_code))
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()