import datetime
from threading import Thread

import akshare as ak
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import pymysql

from lib.args.lib_args import current_time
from lib.args.sql_args import database_rst_company, database_info_target_pro, database_info_target, \
    database_info_target_sit

host = '47.100.162.232'
port = 3306
user = 'root'
password = '1qaz@WSX'
db = 'rst-company'

def async(f):
    def wrapper(*args, **kwargs):
        thr = Thread(target=f, args=args, kwargs=kwargs)
        thr.start()
    return wrapper

def get_stock_code(place):
    conn = pymysql.Connect(
            db=database_rst_company["database"],
            user=database_rst_company["user"],
            password=database_rst_company["password"],
            host=database_rst_company["host"],
            port=database_rst_company["port"]
        )
    cursor = conn.cursor()
    if place == 'hk':
        db_table = 'allstocks_hk'
    elif place == 'us':
        db_table = 'allstocks_us'
    elif place == 'ETF':
        db_table = 'all_funds_sz'
    select_sql = f"""select stock_code, c_name from {db_table} order by stock_code"""
    # select_sql = f"""select stock_code, c_name from {db_table} order by stock_code desc"""
    cursor.execute(select_sql)
    result = cursor.fetchall()
    conn.close()
    return result

def akshare_data(stock_code, c_name, adjust, place):
    try:
        if place == 'hk':
            stock_df = ak.stock_hk_daily(symbol=f"{stock_code}", adjust=adjust)
        else:
            stock_df = ak.stock_us_daily(symbol=f"{stock_code}", adjust=adjust)
        stock_df.insert(0, 'adjust', adjust)
        stock_df.insert(0, 'stock_code', stock_code)
        stock_df.insert(0, 'c_name', c_name)
        stock_df.rename(columns={'close': 't_close'}, inplace=True)
        stock_df.rename(columns={'open': 't_open'}, inplace=True)
        stock_df.rename(columns={'high': 't_high'}, inplace=True)
        stock_df.rename(columns={'low': 't_low'}, inplace=True)
        stock_df.rename(columns={'volume': 't_volume'}, inplace=True)
        # print(stock_df)
        save_to_sql(stock_df, place)
    except Exception as e:
        print(e)


def save_to_sql(stock_df, place):
    conn = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}', encoding='utf8')
    con = conn.connect()
    if place == 'hk':
        # db_table = "allstock_market_info_hk"
        db_table = "allstock_market_info_hk"
    else:
        db_table = "allstock_market_info_us"
    pd.io.sql.to_sql(stock_df, db_table, con, if_exists='append')
    con.close()

# 更新今收盘价缺失值
# @async
def check_y_close(stock_code, place, db):
    if db == 'postgresql':
        conn = psycopg2.connect(
            database=database_info_target["database"],
            user=database_info_target["user"],
            password=database_info_target["password"],
            host=database_info_target["host"],
            port=database_info_target["port"]
        )

        # conn = psycopg2.connect(
        #     database=database_info_target_sit["database"],
        #     user=database_info_target_sit["user"],
        #     password=database_info_target_sit["password"],
        #     host=database_info_target_sit["host"],
        #     port=database_info_target_sit["port"]
        # )
        #
        # conn = psycopg2.connect(
        #     database=database_info_target_pro["database"],
        #     user=database_info_target_pro["user"],
        #     password=database_info_target_pro["password"],
        #     host=database_info_target_pro["host"],
        #     port=database_info_target_pro["port"]
        # )

    else:
        conn = pymysql.Connect(
            db=database_rst_company["database"],
            user=database_rst_company["user"],
            password=database_rst_company["password"],
            host=database_rst_company["host"],
            port=database_rst_company["port"]
        )

    if db == 'postgresql':
        cursor = conn.cursor()
        # 不复权
        adjust = ''
        if place == 'hk':
            db_table = 't_exponent_company_daily_val_hk'
        elif place == 'us':
            db_table = 't_exponent_company_daily_val_us'
        elif place == 'ETF':
            db_table = 't_exponent_company_daily_fund_sz'
        # share_code, end_date, status, dailyzf, dailyzdf, dailycjl, dailyzdj, dailyzgj, dailyzsz, dailykpj, dailyspj, dailyzde, dailyqsp, ext_adjust,  ext_ratio, ext_listing
        # select_sql = f"""select end_date, share_code, dailyspj from {db_table} where share_code=%s and share_code!=%s"""
        # cursor.execute(select_sql, (stock_code, 'null'))

        select_sql = f"""select end_date, share_code, dailyspj from {db_table} where share_code=%s and status=%s and dailyqsp is null"""
        cursor.execute(select_sql, (stock_code, 'valid'))
        result = cursor.fetchall()
        if result == [] or len(result) == 1:
            pass
        else:
            # 测试
            select_sql = f"""select end_date, share_code, dailyspj from {db_table} where share_code=%s and status=%s order by end_date desc"""
            cursor.execute(select_sql, (stock_code, 'valid'))
            result = cursor.fetchall()
            for i in range(0, len(result)):
                if i == len(result)-1:
                    break
                else:
                    t_date, t_share_code, t_dailyspj = result[i]  # 2020-05-10
                    y_date, y_share_code, y_dailyspj = result[i+1]  # 2020-05-09

                    if y_dailyspj == None:
                        pass
                    else:
                        update_sql = f"""update {db_table} set dailyqsp=%s where share_code=%s and end_date=%s"""
                        try:
                            cursor.execute(update_sql,
                                           (y_dailyspj, stock_code, str(t_date)))
                            conn.commit()
                        except Exception as e:
                            print(e)
                            conn.rollback()


                    # rise_fall = '%.5f' % (float(y_dailyspj) - float(t_dailyspj))
                    # if float(y_dailyspj) == 0 or float(y_dailyspj) - float(t_dailyspj) == 0:
                    #     applies = 0
                    # else:
                    #     applies = '%.5f' % ((float(y_dailyspj) - float(t_dailyspj)) / float(y_dailyspj) * 100)
                    #
                    # update_sql = f"""update {db_table} set dailyqsp=%s, dailyzde=%s, dailyzdf=%s where share_code=%s and end_date=%s"""
                    # try:
                    #     cursor.execute(update_sql,
                    #                    (y_dailyspj, rise_fall, applies, stock_code, str(t_date).split(' ')[0]))
                    #     conn.commit()
                    # except Exception as e:
                    #     print(e)
                    #     conn.rollback()


        # 不用算法写
        # select_sql = f"""select end_date, share_code, dailyspj from {db_table} where share_code=%s and share_code!=%s and dailyqsp is null"""
        # cursor.execute(select_sql, (stock_code, 'null'))
        # result = cursor.fetchall()
        # if result != [] and len(result) != 1:
        #     for i in result:
        #         date, stock_code, t_close = i
        #         date = datetime.datetime.strptime(date, '%Y-%m-%d')
        #         one_day = datetime.timedelta(days=1)
        #         y_date = date - one_day
        #         num = 0
        #         while True:
        #             s_sql = f"""select dailyspj from {db_table} where share_code=%s and end_date=%s"""
        #             cursor.execute(s_sql, (stock_code, str(y_date).split(' ')[0]))
        #             result2 = cursor.fetchone()
        #             if result2 != None:
        #                 result2 = result2[0]
        #                 if float(result2) == float(t_close):
        #                     rise_fall = 0
        #                     applies = 0
        #                 else:
        #                     rise_fall = '%.5f' % (float(result2) - float(t_close))
        #                     try:
        #                         applies = '%.5f' % ((float(result2) - float(t_close)) / float(result2) * 100)
        #                     except Exception as e:
        #                         print(e, float(result2), float(t_close))
        #                 update_sql = f"""update {db_table} set dailyqsp=%s, dailyzde=%s, dailyzdf=%s where share_code=%s and end_date=%s"""
        #                 try:
        #                     cursor.execute(update_sql,
        #                                    (t_close, rise_fall, applies, stock_code, str(y_date).split(' ')[0]))
        #                     conn.commit()
        #                 except Exception as e:
        #                     print(e)
        #                     conn.rollback()
        #                 break
        #             else:
        #                 if num > 10:
        #                     break
        #                 else:
        #                     num += 1
        #                     y_date = y_date - one_day
        #         # print(date)
    else:
        cursor = conn.cursor()
        # 不复权
        adjust = ''
        if place == 'hk':
            db_table = 'allstock_market_info_hk'
        else:
            db_table = 'allstock_market_info_us'
        select_sql = f"""select date, stock_code, t_close from {db_table} where stock_code=%s"""
        cursor.execute(select_sql, stock_code)
        result = cursor.fetchall()
        if result != []:
            for i in result:
                date, stock_code, t_close = i
                one_day = datetime.timedelta(days=1)
                y_date = date + one_day
                while True:
                    s_sql = f"""select t_close from {db_table} where stock_code=%s and date=%s and adjust=%s"""
                    cursor.execute(s_sql, (stock_code, y_date, adjust))
                    result2 = cursor.fetchone()
                    if result2 != None:
                        result2 = result2[0]
                        break
                    else:
                        if y_date > current_time:
                            return
                        else:
                            y_date = y_date + one_day
                rise_fall = '%.5f' % (float(t_close) - float(result2))
                applies = '%.5f' % ((float(t_close) - float(result2)) / float(t_close)*100)
                update_sql = f"""update {db_table} set y_close=%s, rise_fall=%s, applies=%s where stock_code=%s and date=%s and adjust=%s"""
                try:
                    cursor.execute(update_sql, (t_close, rise_fall, f'{applies}%', stock_code, y_date, adjust))
                    conn.commit()
                except Exception as e:
                    print(e)
                    conn.rollback()
    conn.close()

# 更新上市地等缺失值
def change_listing(stock_code):
    conn = pymysql.Connect(
        db=database_rst_company["database"],
        user=database_rst_company["user"],
        password=database_rst_company["password"],
        host=database_rst_company["host"],
        port=database_rst_company["port"]
    )
    cursor = conn.cursor()
    db_table = 'allstock_market_info_us'
    select_sql = f"""select category, category_id, stock_code, english_name ,groups, listing from {db_table} where stock_code=%s and date=%s"""
    cursor.execute(select_sql, (stock_code, current_time))
    result = cursor.fetchone()
    category, category_id, stock_code, english_name, groups, listing = result
    update_sql = f"""update {db_table} set category=%s, category_id=%s, english_name=%s, groups=%s, listing=%s where stock_code=%s and groups is null and listing is null"""
    try:
        cursor.execute(update_sql, (category, category_id, english_name, groups, listing, stock_code))
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()

    '''
    stock_item['english_name'] = english_name
    stock_item['groups'] = groups
    stock_item['listing'] = listing
    stock_item['category'] = category
    stock_item['category_id'] = category_id
    '''

def main(place, db):
    result = get_stock_code(place)
    print(result)
    num = 1
    for stock_code, c_name in result:
        print(stock_code, num)
        num += 1
        # 获取数据
        # 未复权
        # akshare_data(stock_code, c_name, '', place)
        # # 后复权
        # akshare_data(stock_code, c_name, 'hfq', place)

        # # 修改昨收缺失
        check_y_close(stock_code, place, db)

        # # 修改上市地缺失
        # change_listing(stock_code)

if __name__ == '__main__':
    place = 'ETF'
    db = 'postgresql'
    main(place, db)
