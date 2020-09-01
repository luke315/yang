import datetime
import time

import psycopg2

from lib.Database_ETF.change_code import list_node, get_node, save_exp_data
from lib.Database_ETF.save_datacenter import save_datacenter_sql
from lib.args.sql_args import database_info_target, database_info_target_sit, database_info_target_pro
from lib.args.lib_args import async_func


# # mysql数据转到postgresql读取数据
from lib.sql_db.sql_all import update_sql_func, insert_sql_func, select_sql_func


def real_mysql_table(spider_name, now_date, conn, data_type):
    # 链接测试库
    cursor = conn.cursor()
    # 选择不同股市数据库table
    if spider_name == 'American_stock_info':
        table = 'allstock_market_info_us'
        field = 'date, c_name, english_name, stock_code, adjust, t_open, t_high, t_low, y_close, t_close, t_volume, rise_fall, applies, amplitude, market_value, ratio, listing'
        where_field = f"date='{now_date}'"
    # ETF
    elif spider_name == 'ETF_market_info':
        if data_type == 'cgmx':
            now_date = datetime.datetime.strptime(now_date, '%Y-%m-%d')
            table = 'all_fund_investment_cgmx'
            field = 'date, fund_code, cgmx_code_name, cgmx_code, cgmx_account, cgmx_hold, cgmx_value'
            where_field = f"date='{now_date}'"
        elif data_type == 'zchy':
            now_date = datetime.datetime.strptime(now_date, '%Y-%m-%d')
            table = 'all_fund_investment_zchy'
            field = 'date, fund_code, zchy_rank, zchy_form, zchy_value, zchy_account, zchy_account_change'
            where_field = f"date='{now_date}'"
        else:
            now_date = datetime.datetime.strptime(now_date, '%Y-%m-%d')
            T_time = datetime.timedelta(days=1)
            table = 'all_fund_market_info_sz'
            field = 'fund_code, s_name, date, t_high, t_low, t_open, t_close, y_close, t_volume, t_volume_price, rise_fall, applies, fund_zzc, fund_zfe, fund_hsl, fund_zjl, fund_dwjz, fund_ljjz, fund_zzl'
            where_field = f"date='{now_date}' or date='{now_date - T_time}'"
            # where_field = f"date='{now_date}'"
    # 港股
    elif spider_name == 'hk_stock_info':
        table = 'allstock_market_info_hk'
        field = 'stock_code, date, c_name, adjust, t_open, t_high, t_low, y_close, t_close, t_volume, t_volume_price, rise_fall, applies'
        where_field = f"date='{now_date}'"
    else:
        print(f'未知爬虫：{spider_name}')
        return

    # 读取数据
    select_sql = f"""select {field} from {table} where {where_field}"""
    cursor.execute(select_sql)
    result = cursor.fetchall()
    return result

# # mysql数据转到postgresql保存数据
@async_func
def save_to_postgresql(spider_name, result, db, data_type):
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
    # 判断为那个爬虫
    if spider_name == 'American_stock_info':
        table = 't_exponent_company_daily_val_us'
    # ETF
    elif spider_name == 'ETF_market_info':
        if data_type == 'cgmx':
            table = 't_exponent_company_daily_fund_sz_cgmx'
        elif data_type == 'zchy':
            table = 't_exponent_company_daily_fund_sz_zchy'
        else:
            table = 't_exponent_company_daily_fund_sz'
    # 港股
    elif spider_name == 'hk_stock_info':
        table = 't_exponent_company_daily_val_hk'
    else:
        print(f'未知爬虫：{spider_name}')
        return
        # 解压赋值
    for i in result:
        if spider_name == 'American_stock_info':
            date, c_name, english_name, stock_code, adjust, t_open, t_high, t_low, y_close, t_close, t_volume, rise_fall, applies, amplitude, market_value, ratio, listing = i

            # 处理格式入库
            date = str(date)
            applies = applies.replace('%', '')
            amplitude = amplitude.replace('%', '')

            if amplitude == 'None':
                applies = '0'

            if applies == 'None':
                applies = '0'

            cursor = conn.cursor()
            select_sql = f"""select * from {table} where share_code=%s and end_date=%s"""
            cursor.execute(select_sql, (stock_code, date))
            if cursor.fetchone() is None:
                # 插入
                insert_sql = f"""insert into {table} (share_code, end_date, status, dailyzf, dailyzdf, dailycjl, dailyzdj, dailyzgj, dailyzsz, dailykpj, dailyspj, dailyzde, dailyqsp, ext_adjust,  ext_ratio, ext_listing) values('%s','%s','%s','%s','%s','%s','%s','%s',%s,'%s',%s,'%s','%s','%s','%s','%s')""" \
                             % (
                                 stock_code, date, 'valid', amplitude, applies, t_volume, t_low, t_high,
                                 market_value, t_open, t_close, rise_fall, y_close, adjust, ratio, listing)
                try:
                    cursor.execute(insert_sql)
                    conn.commit()
                except Exception as e:
                    print(e, stock_code)
                    conn.rollback()
            else:
                update_sql = f"""update {table} set dailyzf=%s, dailyzdf=%s, dailycjl=%s, dailyzdj=%s, dailyzgj=%s, dailyzsz=%s, dailykpj=%s, dailyspj=%s, dailyzde=%s, dailyqsp=%s, ext_ratio=%s where share_code=%s and end_date=%s"""
                try:
                    cursor.execute(update_sql, (
                        amplitude, applies, t_volume, t_low, t_high, market_value, t_open, t_close, rise_fall,
                        y_close, ratio, stock_code, date))
                    conn.commit()
                except Exception as e:
                    print(e, stock_code)
                    conn.rollback()
        elif spider_name == 'hk_stock_info':
            stock_code, date, c_name, adjust, t_open, t_high, t_low, y_close, t_close, t_volume, t_volume_price, rise_fall, applies = i
            # 处理格式入库
            date = str(date)
            applies = applies.replace('%', '')

            if applies == 'None':
                applies = '0'
            cursor = conn.cursor()
            # 查询
            select_sql = f"""select * from {table} where share_code=%s and end_date=%s"""
            cursor.execute(select_sql, (stock_code, date))
            result1 = cursor.fetchone()
            if result1 is None:
                # 插入
                insert_sql = f"""insert into {table} (share_code, end_date, status, dailyzdf, dailycjje, dailycjl, dailyzdj, dailyzgj, dailykpj, dailyspj, dailyzde, dailyqsp, ext_adjust) values('%s','%s','%s','%s','%s','%s','%s',%s,'%s',%s,'%s','%s','%s')""" \
                             % (stock_code, date, 'valid', applies, t_volume_price, t_volume, t_low, t_high, t_open,
                                t_close, rise_fall, y_close, adjust)
                try:
                    cursor.execute(insert_sql)
                    conn.commit()
                except Exception as e:
                    print(e, stock_code)
                    conn.rollback()
            else:
                update_sql = f"""update {table} set dailyzdf=%s, dailycjje=%s, dailycjl=%s, dailyzdj=%s, dailyzgj=%s, dailykpj=%s, dailyspj=%s, dailyzde=%s, dailyqsp=%s where share_code=%s and end_date=%s"""
                try:
                    cursor.execute(update_sql, (
                        applies, t_volume_price, t_volume, t_low, t_high, t_open, t_close, rise_fall, y_close,
                        stock_code, date))
                    conn.commit()
                except Exception as e:
                    print(e, stock_code)
                    conn.rollback()
        elif spider_name == 'ETF_market_info':
            if data_type == 'cgmx':
                table = 't_exponent_company_daily_fund_sz_cgmx'
                date, stock_code, cgmx_code_name, cgmx_code, cgmx_account, cgmx_hold, cgmx_value = i
                date = str(date)
                cgmx_account = change_none(cgmx_account)
                cgmx_hold = change_none(cgmx_hold)
                cgmx_value = change_none(cgmx_value)

                # 拼接字段
                fields = '*'
                where_fields = f"""share_code='{stock_code}' and end_date='{date}' and cgmx_code='{cgmx_code}'"""
                result_select = select_sql_func(conn, table, fields, where_fields)
                # 判断值是否存在
                if result_select == []:
                    # 不存在则插入
                    fields = f"""'{stock_code}','{date}','valid','{cgmx_account}','{cgmx_hold}','{cgmx_value}','{cgmx_code_name}','{cgmx_code}'"""
                    where_fields = 'share_code, end_date, status, cgmx_account, cgmx_hold, cgmx_value, cgmx_code_name, cgmx_code'
                    insert_sql_func(conn, table, fields, where_fields)
                else:
                    # 存在即更新
                    fields = f"""cgmx_account='{cgmx_account}', cgmx_hold='{cgmx_hold}', cgmx_value='{cgmx_value}', cgmx_code_name='{cgmx_code_name}'"""
                    where_fields = f"""share_code='{stock_code}' and end_date='{date}'and cgmx_code='{cgmx_code}'"""
                    update_sql_func(conn, table, fields, where_fields)

            elif data_type == 'zchy':
                table = 't_exponent_company_daily_fund_sz_zchy'
                date, stock_code, zchy_rank, zchy_form, zchy_value, zchy_account, zchy_account_change = i
                date = str(date)
                zchy_value = change_none(zchy_value)
                zchy_account = change_none(zchy_account)
                zchy_account_change = change_none(zchy_account_change)

                # 拼接字段
                fields = '*'
                where_fields = f"""share_code='{stock_code}' and end_date='{date}'and zchy_form='{zchy_form}'"""
                result_select = select_sql_func(conn, table, fields, where_fields)
                # 判断值是否存在
                if result_select == []:
                    # 不存在则插入
                    fields = f"""'{stock_code}','{date}','valid','{zchy_rank}','{zchy_form}','{zchy_value}','{zchy_account}','{zchy_account_change}'"""
                    where_fields = 'share_code, end_date, status, zchy_rank, zchy_form, zchy_value, zchy_account, zchy_account_change'
                    insert_sql_func(conn, table, fields, where_fields)
                else:
                    # 存在即更新
                    fields = f"""zchy_rank='{zchy_rank}', zchy_value='{zchy_value}', zchy_account='{zchy_account}', zchy_account_change='{zchy_account_change}'"""
                    where_fields = f"""share_code='{stock_code}' and end_date='{date}' and zchy_form='{zchy_form}'"""
                    update_sql_func(conn, table, fields, where_fields)
            else:
                # 解压赋值
                stock_code, s_name, date, t_high, t_low, t_open, t_close, y_close, t_volume, t_volume_price, rise_fall, applies, fund_zzc, fund_zfe, fund_hsl, fund_zjl, fund_dwjz, fund_ljjz, fund_zzl = i
                date = str(date)

                fund_dwjz = change_none(fund_dwjz)
                fund_hsl = change_none(fund_hsl)
                fund_zjl = change_none(fund_zjl)
                fund_zzl = change_none(fund_zzl)

                fund_dwjz = change_none(fund_dwjz)
                fund_ljjz = change_none(fund_ljjz)
                t_low = change_none(t_low)
                t_high = change_none(t_high)
                t_open = change_none(t_open)
                t_close = change_none(t_close)
                y_close = change_none(y_close)
                t_volume = change_none(t_volume)
                t_volume_price = change_none(t_volume_price)
                rise_fall = change_none(rise_fall)
                applies = change_none(applies)
                fund_zzc = change_none(fund_zzc)
                fund_zfe = change_none(fund_zfe)


                # 更新到数据库
                list_data = [[stock_code, s_name, date], [t_close, applies, t_volume, t_volume_price, fund_dwjz, fund_ljjz, fund_zzl, fund_hsl, fund_zjl]]
                change_item(list_data, db)

                # 拼接字段
                fields = '*'
                where_fields = f"""share_code='{stock_code}' and end_date='{date}'"""
                result_select = select_sql_func(conn, table, fields, where_fields)
                # 判断值是否存在
                if result_select == []:
                    # 不存在则插入
                    fields = f"""'{stock_code}','{date}','valid','{applies}','{t_volume_price}','{t_volume}','{t_low}','{t_high}','{t_open}','{t_close}','{rise_fall}','{y_close}','{fund_zzc}','{fund_zfe}','{fund_hsl}','{fund_zjl}','{fund_dwjz}','{fund_ljjz}','{fund_zzl}'"""
                    where_fields = 'share_code, end_date, status, dailyzdf, dailycjje, dailycjl, dailyzdj, dailyzgj, dailykpj, dailyspj, dailyzde, dailyqsp, fund_zzc, fund_zfe, fund_hsl, fund_zjl, fund_dwjz, fund_ljjz, fund_zzl'
                    insert_sql_func(conn, table, fields, where_fields)
                else:
                    # 存在即更新
                    fields = f"""dailyzdf='{applies}', dailycjje='{t_volume_price}', dailycjl='{t_volume}', dailyzdj='{t_low}', dailyzgj='{t_high}', dailykpj='{t_open}', dailyspj='{t_close}', dailyzde='{rise_fall}', dailyqsp='{y_close}', fund_zzc='{fund_zzc}', fund_zfe='{fund_zfe}', fund_hsl='{fund_hsl}', fund_zjl='{fund_zjl}', fund_dwjz='{fund_dwjz}', fund_ljjz='{fund_ljjz}', fund_zzl='{fund_zzl}'"""
                    where_fields = f"""share_code='{stock_code}' and end_date='{date}'"""
                    update_sql_func(conn, table, fields, where_fields)
    conn.close()

# 更新数据库datacenter1
def change_item(list_data, db):
    frequency = 'DATE'
    stock_code, s_name, the_date = list_data[0]
    code_dict = get_node('ETF', stock_code, '')
    # print(code_dict)
    save_exp_data(code_dict, s_name, db)

    for i in range(0, len(list_data[1])):
        exponent_code = get_node('ETF', stock_code, list_node[i])
        exponent_value = list_data[1][i]
        item = {'exponent_code': exponent_code, 'exponent_value': exponent_value, 'the_date': the_date, 'frequency': frequency}
        # print(item)
        save_datacenter_sql(item, db)


def change_none(value):
    if value == None or value == '--%' or value == '':
        value = 0
    else:
        value = str(value).replace('%', '')
    return value


