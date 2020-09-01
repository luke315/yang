# 更新股票代码mysql
import psycopg2
import pymysql
from lib.args.lib_args import async_func
from lib.args.sql_args import database_rst_company, database_info_target, database_info_target_sit, database_info_target_pro


@async_func
def update_stock_code_mysql(item, table):
    conn_mysql = pymysql.Connect(
        database=database_rst_company["database"],
        user=database_rst_company["user"],
        password=database_rst_company["password"],
        host=database_rst_company["host"],
        port=database_rst_company["port"]
    )
    cursor = conn_mysql.cursor()
    select_sql = f"""select stock_code from {table} where stock_code=%s"""
    cursor.execute(select_sql, item['stock_code'])
    if table == 'all_funds_sz':
        if cursor.fetchone() is None:
            # 插入
            insert_sql = f"""insert into {table} (stock_code, c_name, s_name) values('%s','%s','%s')""" \
                         % (item['stock_code'], item['stock_name'], item['fund_sname'])
            try:
                cursor.execute(insert_sql)
                conn_mysql.commit()
            except Exception as e:
                print(e)
                conn_mysql.rollback()
        else:
            update_sql = f"""update {table} set c_name=%s, s_name=%s where stock_code=%s"""
            try:
                cursor.execute(update_sql, (item['stock_name'], item['fund_sname'], item['stock_code']))
                conn_mysql.commit()
            except Exception as e:
                print(e)
                conn_mysql.rollback()
    else:
        if cursor.fetchone() is None:
            # 插入
            insert_sql = f"""insert into {table} (stock_code, c_name) values('%s','%s')""" \
                         % (item['stock_code'], item['stock_name'])
            try:
                cursor.execute(insert_sql)
                conn_mysql.commit()
            except Exception as e:
                print(e)
                conn_mysql.rollback()
        else:
            update_sql = f"""update {table} set c_name=%s where stock_code=%s"""
            try:
                cursor.execute(update_sql, (item['stock_name'], item['stock_code']))
                conn_mysql.commit()
            except Exception as e:
                print(e)
                conn_mysql.rollback()
    conn_mysql.close()

# 更新股票代码postgresql
@async_func
def update_stock_code_postgresql(item, table, db):
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
    else:
        conn = psycopg2.connect(
            database=database_info_target_pro["database"],
            user=database_info_target_pro["user"],
            password=database_info_target_pro["password"],
            host=database_info_target_pro["host"],
            port=database_info_target_pro["port"]
        )
    cursor = conn.cursor()

    select_sql = f"""select code from {table} where code=%s and status=%s"""
    cursor.execute(select_sql, (item['stock_code'], 'valid'))

    item['stock_name'] = item['stock_name'].replace('"', '').replace("'", '')
    if table == 't_exponent_company_fund_sz':
        if cursor.fetchone() is None:
            # 插入
            insert_sql = f"""insert into {table} (code, parent_code, exponent_name, desc_s, ext4, ext9) values('%s','%s','%s','%s','%s','%s')""" \
                         % (item['stock_code'], 'COMPANY', item['fund_sname'], item['stock_name'], 'leaf',
                            'FROM_ROOT')
            try:
                cursor.execute(insert_sql)
                conn.commit()
            except Exception as e:
                print(e, item['stock_name'])
                conn.rollback()
        else:
            update_sql = f"""update {table} set exponent_name=%s, desc_s=%s where code=%s"""
            try:
                cursor.execute(update_sql, (item['fund_sname'], item['stock_name'], item['stock_code']))
                conn.commit()
            except Exception as e:
                print(e, item['stock_name'])
                conn.rollback()
    else:
        if cursor.fetchone() is None:
            # 插入
            insert_sql = f"""insert into {table} (code, parent_code, exponent_name, ext4, ext9) values('%s','%s','%s','%s','%s')""" \
                         % (item['stock_code'], 'COMPANY', item['stock_name'], 'leaf',
                            'FROM_ROOT')
            try:
                cursor.execute(insert_sql)
                conn.commit()
            except Exception as e:
                print(e, item['stock_name'])
                conn.rollback()
        else:
            update_sql = f"""update {table} set exponent_name=%s where code=%s"""
            try:
                cursor.execute(update_sql, (item['stock_name'], item['stock_code']))
                conn.commit()
            except Exception as e:
                print(e, item['stock_name'])
                conn.rollback()
    conn.close()