import datetime
import logging
import psycopg2
from lib.args.lib_args import async_func
from lib.args.sql_args import database_info_target, database_info_target_sit, database_info_target_pro
from lib.stock_classify_setting import md5_sql


# @async_func
def save_exponent_sql(item, db):
    table = 't_exponent_fund_sz'
    if db == '232':
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
    elif db == 'pro':
        conn = psycopg2.connect(
            database=database_info_target_pro["database"],
            user=database_info_target_pro["user"],
            password=database_info_target_pro["password"],
            host=database_info_target_pro["host"],
            port=database_info_target_pro["port"]
        )
    else:
        return
    cursor = conn.cursor()

    epx_code = item['epx_code']
    epx_parent_code = item['epx_parent_code']
    epx_name = item['epx_name']
    epx_units = item['epx_units']
    epx_frequency = item['epx_frequency']

    if item['epx_frequency'] != '':
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

    # 更新root表
    # etf_db_root(conn, item)
    conn.close()


def etf_db_root(conn, item):
    top = 'ssgscd06-3d77-4efb-a78d-ad9a9cea3d80'

    table = 't_exponent_fund_sz'
    str_type = 'ETF_db_data'

    if item['epx_code'] == 'ETF_start':
        save_root(conn, md5_sql('ETF', str_type), top, 'ETF', table, '')



# 分类写入root表
def save_root(conn, code, parent_code, exponent_name, ref_table, ref_exponent_code):
    table = 't_exponent_root'
    if ref_table == 't_exponent_fund_sz':
        ext1 = '上市公司'
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