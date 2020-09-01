import datetime
import re

import psycopg2

from lib.db_args import database_info_target, database_info_target2
from lib.md5 import md5_sql


eia_table_name = 't_exponent_eia_copy1'
eia_value_table_name = 't_exponent_value_eia_copy1'

def read_data():
    with open(r'a.txt', 'r', encoding='utf8') as f:
        while True:
            res = f.readline().replace('null', '""')
            if re.findall('series_id', res) != []:
                get_leaf_or_node(eval(res), 'leaf')
            elif re.findall('category_id', res) != []:
                get_leaf_or_node(eval(res), 'node')
            else:
                break

# 获取根节点
def get_leaf_or_node(data, type_data):
    if type_data == 'leaf':
        series_id = data['series_id']
        name = data['name']
        units = data['units']
        frequency = change_frequency(data['f'])
        data_list = data['data']  # [0] = date, [1] = value
        # 保存leaf指标， 父节点暂时设为空  code, parent_code, desc_s, status, ext1, ext2, ext3, ext4
        save_to_postgresql((series_id, '', name, 'invalid', units, '', frequency, 'LEAF'), 'exponent')
        for i in data_list:
            eia_date = i[0]
            eia_value = i[1]
            item = {'epx_code': series_id, 'frequency': frequency, 'epx_value': eia_value, 'period': eia_date}
            save_to_postgresql(item, 'value')
    else:
        pass

# 修改频率字段
def change_frequency(res):
    if res == 'A':
        frequency = 'YEAR'
    elif res == 'Q':
        frequency = 'SEASON'
    elif res == 'M':
        frequency = 'MONTH'
    elif res == 'W':
        frequency = 'WEEK'
    elif res == 'H':
        frequency = 'HOUR'
    elif res == 'HL':
        frequency = 'HOUR'
    elif res == '4':
        frequency = 'WEEK'
    else:
        frequency = 'DATE'
    return frequency

# 时间类型,存储值
def save_exp_value(item):
    conn = psycopg2.connect(
        database=database_info_target2['database'],
        user=database_info_target2['user'],
        password=database_info_target2['password'],
        host=database_info_target2['host'],
        port=database_info_target2['port'])
    cursor = conn.cursor()

    item['epx_code'] = md5_sql(item['epx_code'])

    if item['frequency'] == 'YEAR':
        select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_year=%s"""
        cursor.execute(select_sql, (item['epx_code'], str(item['epx_value']), item['period']))
        result = cursor.fetchone()
        # print(result)
        if result is None:
            sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_year) values('%s','%s','%s')""" \
                  % (item['epx_code'], item['epx_value'], item['period'])
            try:
                cursor.execute(sql)
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
        else:
            sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_year=%s"""
            try:
                cursor.execute(sql, (item['epx_value'], item['epx_code'], item['period']))
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
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
        cursor.execute(select_sql, (item['epx_code'], str(item['epx_value']), season))
        result = cursor.fetchone()
        # print(result)
        if result is None:
            sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_season) values('%s','%s','%s')""" \
                  % (item['epx_code'], item['epx_value'], season)
            try:
                cursor.execute(sql)
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
        else:
            sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_season=%s"""
            try:
                cursor.execute(sql, (item['epx_value'], item['epx_code'], season))
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
    elif item['frequency'] == 'MONTH':
        select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_month=%s"""
        cursor.execute(select_sql, (item['epx_code'], str(item['epx_value']), item['period']))
        result = cursor.fetchone()
        # print(result)
        if result is None:
            sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_month) values('%s','%s','%s')""" \
                  % (item['epx_code'], item['epx_value'], item['period'])
            try:
                cursor.execute(sql)
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
        else:
            sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_month=%s"""
            try:
                cursor.execute(sql, (item['epx_value'], item['epx_code'], item['period']))
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
    else:
        # 如果是小时，period不处理
        if item['frequency'] == 'HOUR':
            date = item['period']
        else:
            # 频率为周和天，处理成2020-05-20类似格式
            i = item['period']
            date = f'{i[0:4]}-{i[4:6]}-{i[6] + i[7]}'
        select_sql = f"""SELECT * FROM {eia_value_table_name} WHERE exponent_code=%s and exponent_value=%s and the_date=%s"""
        cursor.execute(select_sql, (item['epx_code'], str(item['epx_value']), date))
        result = cursor.fetchone()
        # print(result)
        if result is None:
            sql = f"""insert into {eia_value_table_name} (exponent_code, exponent_value, the_date) values('%s','%s','%s')""" \
                  % (item['epx_code'], item['epx_value'], date)
            try:
                cursor.execute(sql)
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
        else:
            sql = f"""update {eia_value_table_name} set exponent_value=%s where exponent_code=%s and the_date=%s"""
            try:
                cursor.execute(sql, (item['epx_value'], item['epx_code'], date))
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
        eia_exponent_update_time(item['epx_code'])


def save_exp(item):
    conn = psycopg2.connect(
        database=database_info_target['database'],
        user=database_info_target['user'],
        password=database_info_target['password'],
        host=database_info_target['host'],
        port=database_info_target['port'])
    cursor = conn.cursor()
    ext2 = str({'id': item['code']})
    # 查看
    if item['status'] != 'invalid':
        pass
        # 处理
        # 1查询库里根节点url
        # 2形成父子目录


    select_sql = f"""SELECT * FROM {eia_table_name} WHERE code = %s AND parent_code = %s"""
    cursor.execute(select_sql, (item['code'], item['parent_code']))
    result = cursor.fetchone()
    # print(result)
    item['code'] = md5_sql(item['epx_code'])
    item['parent_code'] = md5_sql(item['epx_parent_code'])
    if result is None:
        # 插入
        sql = f"""insert into {eia_table_name}(code, parent_code, desc_s, status, ext1, ext2, ext3, ext4) values('%s','%s','%s','%s','%s','%s','%s','%s')""" \
              % (item['code'], item['parent_code'], item['desc_s'].replace('\"', '').replace('\'', ''),
                 'valid', item['ext1'], ext2, item['ext3'], item['ext4'])
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
    else:
        sql = f"""update {eia_table_name} set desc_s=%s, ext1=%s, ext2=%s, ext3=%s, ext4=%s WHERE code = %s AND parent_code = %s"""
        try:
            cursor.execute(sql, (
                item['desc_s'].replace('\"', '').replace('\'', ''), item['epx_units'], item['ext1'], ext2, item['ext3'], item['ext4'], item['code'], item['parent_code']))
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()


# 更新指标表时间
def eia_exponent_update_time(exponent_code):
    conn = psycopg2.connect(
        database=database_info_target['database'],
        user=database_info_target['user'],
        password=database_info_target['password'],
        host=database_info_target['host'],
        port=database_info_target['port'])
    cursor = conn.cursor()
    sql = f"""update t_exponent_eia set update_time=%s WHERE code=%s AND status=%s"""
    try:
        cursor.execute(sql, (str(datetime.datetime.now()), exponent_code, 'valid'))
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()

def save_to_postgresql(data, db):
    # (series_id, '', name, 'invalid', units, '', frequency, 'LEAF')
    # parent_code, desc_s, status, ext1, ext2, ext3, ext4
    if db == 'value':
        save_exp_value(data)
    else:
        item = {'code': data[0], 'parent_code': data[1], 'desc_s': data[2], 'status': data[3],  'ext1': data[4], 'ext2': data[5], 'ext3': data[6], 'ext4': data[7]}
        save_exp(item)

