import datetime
import psycopg2

from lib.args.lib_args import async_func
from lib.args.sql_args import database_info_target2, database_info_target, database_info_target_sit, \
    database_info_target_pro, database_info_target_sit_datacenter1, database_info_target_pro_datacenter1
from lib.sql_db.sql_all import select_sql_func, insert_sql_func, update_sql_func

@async_func
def save_datacenter_sql(item, db):
    table = 't_exponent_value_fund_sz'
    if db == '232':
        conn = psycopg2.connect(
            database=database_info_target2["database"],
            user=database_info_target2["user"],
            password=database_info_target2["password"],
            host=database_info_target2["host"],
            port=database_info_target2["port"]
        )
    elif db == 'sit':
        conn = psycopg2.connect(
            database=database_info_target_sit_datacenter1["database"],
            user=database_info_target_sit_datacenter1["user"],
            password=database_info_target_sit_datacenter1["password"],
            host=database_info_target_sit_datacenter1["host"],
            port=database_info_target_sit_datacenter1["port"]
        )
    elif db == 'pro':
        conn = psycopg2.connect(
            database=database_info_target_pro_datacenter1["database"],
            user=database_info_target_pro_datacenter1["user"],
            password=database_info_target_pro_datacenter1["password"],
            host=database_info_target_pro_datacenter1["host"],
            port=database_info_target_pro_datacenter1["port"]
        )
    else:
        return

    exponent_code = item['exponent_code']
    exponent_value = item['exponent_value']
    date_type_value = item['the_date']

    # 插入 the_year, the_season, the_month, the_date
    if item['frequency'] == 'YEAR':
        date_type = 'the_year'
    elif item['frequency'] == 'SEASON':
        date_type = 'the_season'
    elif item['frequency'] == 'MONTH':
        date_type = 'the_month'
    elif item['frequency'] == 'DATE':
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
        exponent_update_time(exponent_code, db, 't_exponent_fund_sz')
    else:
        # 值相同则忽略
        if result_select[0] != exponent_value:
            # 存在即更新
            fields = f""" exponent_value='{exponent_value}', update_time='{str(datetime.datetime.now())}'"""
            where_fields = f"""exponent_code='{exponent_code}' and {date_type}='{date_type_value}'"""
            update_sql_func(conn, table, fields, where_fields)
            # 更新exp表
            exponent_update_time(exponent_code, db, 't_exponent_fund_sz')
    conn.close()
    return item

def exponent_update_time(exponent_code, db, table):
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
    sql = f"""update {table} set update_time=%s WHERE code=%s AND status=%s"""
    try:
        cursor.execute(sql, (str(datetime.datetime.now()), exponent_code, 'valid'))
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    conn.close()


