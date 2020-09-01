# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import re

import psycopg2
import pymysql
from lib.args.lib_args import async_func
from lib.args.sql_args import database_info_target, database_info_target_sit, database_info_target_pro, database_rst_company
from lib.delete_log import delete_logs_star
from lib.stock_classify_setting import get_p_c, stock_classify
from lib.update_stock import update_stock_code_mysql, update_stock_code_postgresql
from lib.update_stock_info_to_postgresql import real_mysql_table, save_to_postgresql
from pro.items import Company_Profile_Item, Company_IPO_Item, Company_Income_Item


# # 异步调用
# def async_func(f):
#     def wrapper(*args, **kwargs):
#         thr = Thread(target=f, args=args, kwargs=kwargs)
#         thr.start()
#     return wrapper

# 网易沪深ETF指标值
class ETF_pipline(object):
    now_date = ''
    conn_mysql = None
    cursor = None
    cursor_postgresql = None
    conn_postgresql = None
    report_date = None

    def open_spider(self, spider):
        self.conn_mysql = pymysql.Connect(
            database=database_rst_company["database"],
            user=database_rst_company["user"],
            password=database_rst_company["password"],
            host=database_rst_company["host"],
            port=database_rst_company["port"]
        )
        # if spider.name == 'ETF_market_info_history':
        #     self.conn_postgresql = psycopg2.connect(
        #         database=database_info_target["database"],
        #         user=database_info_target["user"],
        #         password=database_info_target["password"],
        #         host=database_info_target["host"],
        #         port=database_info_target["port"]
        #     )

    def close_spider(self, spider):
        if spider.name == 'ETF_market_info':
            # 爬虫结束，同步今天数据
            save_to_postgresql_main(spider.name, self.now_date, self.conn_mysql, '')
            if self.report_date == True:
                save_to_postgresql_main(spider.name, self.now_date, self.conn_mysql, 'cgmx')
                save_to_postgresql_main(spider.name, self.now_date, self.conn_mysql, 'zchy')
        self.conn_mysql.close()
        print('close_spider')
        delete_logs_star()

    def process_item(self, item, spider):
        self.conn_mysql.ping(reconnect=True)
        self.cursor = self.conn_mysql.cursor()
        if spider.name == 'ETF_market_info':
            if item['report_date'] == '1':
                self.now_date = item['fund_date']
                # 更新root分类表
                stock_classify_main(item, 'ETF')

                # 更新postgresql基金代码
                update_stock_code_postgresql_main(item, 't_exponent_company_fund_sz')
                # 转义
                item['stock_name'] = pymysql.escape_string(item['stock_name'])
                item['stock_code'] = pymysql.escape_string(item['stock_code'])
                # 更新mysql基金代码
                update_stock_code_mysql(item, 'all_funds_sz')

                # 插入
                sql = """select * from all_fund_market_info_sz where fund_code=%s and date=%s"""
                self.cursor.execute(sql, (item['stock_code'], item['fund_date']))
                result = self.cursor.fetchone()
                if result == None:
                    sql = 'insert into all_fund_market_info_sz (date, name, s_name, fund_code, t_open, t_high, t_low, t_close, y_close, t_volume, t_volume_price, rise_fall, applies, fund_zfe, fund_zzc) values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' \
                          % (item['fund_date'], item['fund_sname'], item['stock_name'], item['stock_code'],
                             item['fund_open'], item['fund_high'], item['fund_low'], item['fund_close'],
                             item['fund_y_close'], item['fund_volume_num'], item['fund_volume_price'],
                             item['fund_up_down'], item['fund_up_down_percent'], item['fund_zfe'], item['fund_zzc'])
                    try:
                        self.cursor.execute(sql)
                        self.conn_mysql.commit()
                    except Exception as e:
                        print(e)
                        self.conn_mysql.rollback()
                else:
                    sql = """update all_fund_market_info_sz set name=%s, s_name=%s, t_open=%s, t_high=%s, t_low=%s, t_close=%s, y_close=%s, t_volume=%s, t_volume_price=%s, rise_fall=%s, applies=%s, fund_zfe=%s, fund_zzc=%s where fund_code=%s and date=%s"""
                    try:
                        self.cursor.execute(sql, (
                            item['fund_sname'], item['stock_name'], item['fund_open'], item['fund_high'], item['fund_low'],
                            item['fund_close'], item['fund_y_close'], item['fund_volume_num'], item['fund_volume_price'],
                            item['fund_up_down'], item['fund_up_down_percent'], item['fund_zfe'], item['fund_zzc'],
                            item['stock_code'], item['fund_date']))
                        self.conn_mysql.commit()
                    except Exception as e:
                        print(e)
                        self.conn_mysql.rollback()

                # 更新昨日数据（换手率，折价率）
                self.update_y_data(item)
            else:
                # 投资组合
                if item['fund_date'] == item['report_date']:
                    self.report_date = True
                    self.hold_fund(item)

        elif spider.name == 'ETF_market_info_history':
            # 投资组合历史数据
            if item['report_date'] == '1':
                if item['fund_jzzs_time'] == '':
                    sql = """select * from all_fund_market_info_sz where fund_code=%s and date=%s"""
                    self.cursor.execute(sql, (item['stock_code'], item['fund_zyjl_time']))
                    result = self.cursor.fetchone()
                    if result == None:
                        sql = 'insert into all_fund_market_info_sz (date, name, s_name, fund_code, t_close, t_volume, t_volume_price, applies, fund_hsl, fund_zjl) values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' \
                              % (item['fund_zyjl_time'], item['fund_sname'], item['stock_name'], item['stock_code'],
                                 item['fund_close'], item['fund_volume_num'], item['fund_volume_price'],
                                 item['fund_up_down_percent'], item['fund_hsl'], item['fund_zjl'])
                        try:
                            self.cursor.execute(sql)
                            self.conn_mysql.commit()
                        except Exception as e:
                            print(e)
                            self.conn_mysql.rollback()
                    else:
                        sql = """update all_fund_market_info_sz set name=%s, s_name=%s, t_close=%s, t_volume=%s, t_volume_price=%s, applies=%s, fund_hsl=%s, fund_zjl=%s where fund_code=%s and date=%s"""
                        try:
                            self.cursor.execute(sql, (
                                item['fund_sname'], item['stock_name'], item['fund_close'], item['fund_volume_num'],
                                item['fund_volume_price'], item['fund_up_down_percent'],
                                item['fund_hsl'], item['fund_zjl'], item['stock_code'], item['fund_zyjl_time']))
                            self.conn_mysql.commit()
                        except Exception as e:
                            print(e)
                            self.conn_mysql.rollback()
                elif item['fund_zyjl_time'] == '':
                    sql = """select * from all_fund_market_info_sz where fund_code=%s and date=%s"""
                    self.cursor.execute(sql, (item['stock_code'], item['fund_jzzs_time']))
                    result = self.cursor.fetchone()
                    if result == None:
                        sql = 'insert into all_fund_market_info_sz (date, name, s_name, fund_code, fund_dwjz, fund_ljjz, fund_zzl) values("%s","%s","%s","%s","%s","%s","%s")' \
                              % (item['fund_jzzs_time'], item['fund_sname'], item['stock_name'], item['stock_code'],
                                 item['fund_dwjz'], item['fund_ljjz'], item['fund_zzl'])
                        try:
                            self.cursor.execute(sql)
                            self.conn_mysql.commit()
                        except Exception as e:
                            print(e)
                            self.conn_mysql.rollback()
                    else:
                        sql = """update all_fund_market_info_sz set name=%s, s_name=%s, fund_dwjz=%s, fund_ljjz=%s, fund_zzl=%s where fund_code=%s and date=%s"""
                        try:
                            self.cursor.execute(sql, (
                                item['fund_sname'], item['stock_name'], item['fund_dwjz'], item['fund_ljjz'], item['fund_zzl'],
                                item['stock_code'], item['fund_jzzs_time']))
                            self.conn_mysql.commit()
                        except Exception as e:
                            print(e)
                            self.conn_mysql.rollback()
                # 更新postgresql
                self.update_postgresql_history(item)
            else:
                self.hold_fund(item)

        return item


    # 历史数据
    def update_postgresql_history(self, item):
        self.conn_postgresql = psycopg2.connect(
            database=database_info_target["database"],
            user=database_info_target["user"],
            password=database_info_target["password"],
            host=database_info_target["host"],
            port=database_info_target["port"]
        )
        self.cursor_postgresql = self.conn_postgresql.cursor()
        if item['fund_jzzs_time'] == '':
            if re.findall('--', item['fund_zjl']) != []:
                item['fund_zjl'] = '0'
            if re.findall('--', item['fund_up_down_percent']) != []:
                item['fund_up_down_percent'] = '0'
            item['fund_up_down_percent'] = item['fund_up_down_percent'].replace('%', '')
            item['fund_hsl'] = item['fund_hsl'].replace('%', '')
            item['fund_zjl'] = item['fund_zjl'].replace('%', '')

            sql = """select * from t_exponent_company_daily_fund_sz where share_code=%s and end_date=%s"""
            self.cursor_postgresql.execute(sql, (item['stock_code'], item['fund_zyjl_time']))
            result = self.cursor_postgresql.fetchone()
            if result == None:
                sql = """insert into t_exponent_company_daily_fund_sz (share_code, end_date, status, dailyspj, dailycjl, dailycjje, dailyzdf, fund_hsl, fund_zjl) values('%s','%s','%s','%s','%s','%s','%s','%s','%s')""" \
                      % (item['stock_code'], item['fund_zyjl_time'], 'valid',
                         item['fund_close'], item['fund_volume_num'], item['fund_volume_price'],
                         item['fund_up_down_percent'], item['fund_hsl'], item['fund_zjl'])
                try:
                    self.cursor_postgresql.execute(sql)
                    self.conn_postgresql.commit()
                except Exception as e:
                    print(e)
                    self.conn_postgresql.rollback()
            else:
                sql = """update t_exponent_company_daily_fund_sz set dailyspj=%s, dailycjl=%s, dailycjje=%s, dailyzdf=%s, fund_hsl=%s, fund_zjl=%s where share_code=%s and end_date=%s"""
                try:
                    self.cursor_postgresql.execute(sql, (
                        item['fund_close'], item['fund_volume_num'],
                        item['fund_volume_price'], item['fund_up_down_percent'],
                        item['fund_hsl'], item['fund_zjl'], item['stock_code'], item['fund_zyjl_time']))
                    self.conn_postgresql.commit()
                except Exception as e:
                    print(e)
                    self.conn_postgresql.rollback()
        elif item['fund_zyjl_time'] == '':
            item['fund_zzl'] = item['fund_zzl'].replace('%', '')

            sql = """select * from t_exponent_company_daily_fund_sz where share_code=%s and end_date=%s"""
            self.cursor_postgresql.execute(sql, (item['stock_code'], item['fund_jzzs_time']))
            result = self.cursor_postgresql.fetchone()
            if result == None:
                sql = """insert into t_exponent_company_daily_fund_sz (share_code, end_date, status, fund_dwjz, fund_ljjz, fund_zzl) values('%s','%s','%s"','%s','%s','%s')""" \
                      % (item['stock_code'], item['fund_jzzs_time'], 'valid',
                         item['fund_dwjz'], item['fund_ljjz'], item['fund_zzl'])
                try:
                    self.cursor_postgresql.execute(sql)
                    self.conn_postgresql.commit()
                except Exception as e:
                    print(e)
                    self.conn_postgresql.rollback()
            else:
                sql = """update t_exponent_company_daily_fund_sz set fund_dwjz=%s, fund_ljjz=%s, fund_zzl=%s where share_code=%s and end_date=%s"""
                try:
                    self.cursor_postgresql.execute(sql, (
                        item['fund_dwjz'], item['fund_ljjz'], item['fund_zzl'],
                        item['stock_code'], item['fund_jzzs_time']))
                    self.conn_postgresql.commit()
                except Exception as e:
                    print(e)
                    self.conn_postgresql.rollback()
        self.conn_postgresql.close()

    # 投资组合
    def hold_fund(self, item):
        if item['type_cgmx_or_zchy'] == 'cgmx':
            sql = """select * from all_fund_investment_cgmx where fund_code=%s and date=%s and cgmx_code=%s"""
            self.cursor.execute(sql, (item['stock_code'], item['report_date'], item['cgmx_code']))
            result = self.cursor.fetchone()
            if result == None:
                sql = 'insert into all_fund_investment_cgmx (date, name, s_name, fund_code, cgmx_code_name, cgmx_code, cgmx_account, cgmx_hold, cgmx_value) values("%s","%s","%s","%s","%s","%s","%s","%s","%s")' \
                      % (item['report_date'], item['fund_sname'], item['stock_name'], item['stock_code'],
                         item['cgmx_code_name'], item['cgmx_code'], item['cgmx_account'], item['cgmx_hold'],
                         item['cgmx_value'])
                try:
                    self.cursor.execute(sql)
                    self.conn_mysql.commit()
                except Exception as e:
                    print(e)
                    self.conn_mysql.rollback()
            else:
                sql = """update all_fund_investment_cgmx set name=%s, s_name=%s, cgmx_code_name=%s, cgmx_code=%s, cgmx_account=%s, cgmx_hold=%s, cgmx_value=%s where fund_code=%s and date=%s and cgmx_code=%s"""
                try:
                    self.cursor.execute(sql, (
                         item['fund_sname'], item['stock_name'], item['cgmx_code_name'], item['cgmx_code'], item['cgmx_account'], item['cgmx_hold'],
                         item['cgmx_value'], item['stock_code'], item['report_date'], item['cgmx_code']))
                    self.conn_mysql.commit()
                except Exception as e:
                    print(e)
                    self.conn_mysql.rollback()
        elif item['type_cgmx_or_zchy'] == 'zchy':
            sql = """select * from all_fund_investment_zchy where fund_code=%s and date=%s and zchy_form=%s"""
            self.cursor.execute(sql, (item['stock_code'], item['report_date'], item['zchy_form']))
            result = self.cursor.fetchone()
            if result == None:
                sql = 'insert into all_fund_investment_zchy (date, name, s_name, fund_code, zchy_rank, zchy_form, zchy_value, zchy_account, zchy_account_change) values("%s","%s","%s","%s","%s","%s","%s","%s","%s")' \
                      % (item['report_date'], item['fund_sname'], item['stock_name'], item['stock_code'],
                         item['zchy_rank'], item['zchy_form'], item['zchy_value'],
                         item['zchy_account'], item['zchy_account_change'])
                try:
                    self.cursor.execute(sql)
                    self.conn_mysql.commit()
                except Exception as e:
                    print(e)
                    self.conn_mysql.rollback()
            else:
                sql = """update all_fund_investment_zchy set name=%s, s_name=%s, zchy_rank=%s, zchy_form=%s, zchy_value=%s, zchy_account=%s, zchy_account_change=%s where fund_code=%s and date=%s and zchy_form=%s"""
                try:
                    self.cursor.execute(sql, (
                        item['fund_sname'], item['stock_name'], item['zchy_rank'], item['zchy_form'],
                        item['zchy_value'], item['zchy_account'], item['zchy_account_change'],
                        item['stock_code'], item['report_date'], item['zchy_form']))
                    self.conn_mysql.commit()
                except Exception as e:
                    print(e)
                    self.conn_mysql.rollback()

    # 更新折价率和增长率
    def update_y_data(self, item):
        # 更新换手率，折溢价，净值
        if item['fund_jzzs_time'] == item['fund_zyjl_time'] and item['fund_jzzs_time'] != '':
            sql = """update all_fund_market_info_sz set fund_dwjz=%s, fund_ljjz=%s, fund_zzl=%s ,fund_hsl=%s, fund_zjl=%s where fund_code=%s and date=%s"""
            try:
                self.cursor.execute(sql, (item['fund_dwjz'], item['fund_ljjz'], item['fund_zzl'],
                                          item['fund_y_hsl'], item['fund_y_zjl'], item['stock_code'],
                                          item['fund_jzzs_time']))
                self.conn_mysql.commit()
            except Exception as e:
                print(e)
                self.conn_mysql.rollback()
        else:
            if item['fund_jzzs_time'] != '':
                sql = """update all_fund_market_info_sz set fund_dwjz=%s, fund_ljjz=%s, fund_zzl=%s where fund_code=%s and date=%s"""
                try:
                    self.cursor.execute(sql, (
                    item['fund_dwjz'], item['fund_ljjz'], item['fund_zzl'], item['stock_code'], item['fund_jzzs_time']))
                    self.conn_mysql.commit()
                except Exception as e:
                    print(e)
                    self.conn_mysql.rollback()
            elif item['fund_zyjl_time'] != '':
                sql = """update all_fund_market_info_sz set fund_hsl=%s, fund_zjl=%s where fund_code=%s and date=%s"""
                try:
                    self.cursor.execute(sql,
                                        (item['fund_y_hsl'], item['fund_y_zjl'], item['stock_code'], item['fund_zyjl_time']))
                    self.conn_mysql.commit()
                except Exception as e:
                    print(e)
                    self.conn_mysql.rollback()


# 新浪沪深上市公司PDF写入
class MysqlPipeline:
    conn = None
    cursor = None

    def open_spider(self, spider):
        self.conn = pymysql.Connect(
            database=database_rst_company["database"],
            user=database_rst_company["user"],
            password=database_rst_company["password"],
            host=database_rst_company["host"],
            port=database_rst_company["port"]
        )

    def close_spider(self, spider):
        self.conn.close()

    def process_item(self, item, spider):
        self.conn.ping(reconnect=True)
        # print((item['stock_code'], item['file_name']))
        self.cursor = self.conn.cursor()
        # 插入
        sql = """select * from stocks_file where stock_code=%s and file_name=%s"""
        self.cursor.execute(sql, (item['stock_code'], item['file_name']))
        result = self.cursor.fetchone()
        if result == None:
            sql = 'insert into stocks_file (stock_code, company, pdf_file, html_file, file_name, file_type, file_date) values("%s","%s","%s","%s","%s","%s","%s")' \
                  % (item['stock_code'], item['company_name'], item['pdf_file'], item['html_file'],
                     item['file_name'],
                     item['file_type'], item['file_date'])
            try:
                self.cursor.execute(sql)
                self.conn.commit()
            except Exception as e:
                print(e)
                self.conn.rollback()
        return item


# 写入postgresql
class Psycopg2pipline(object):
    conn = None
    cursor = None

    def process_item(self, item, spider):
        self.conn = psycopg2.connect(
            database=database_info_target["database"],
            user=database_info_target["user"],
            password=database_info_target["password"],
            host=database_info_target["host"],
            port=database_info_target["port"]
        )

        self.cursor = self.conn.cursor()
        if isinstance(item, Company_Profile_Item):
            # 查看
            sql = """select stock_code from t_exponent_company_profile where stock_code=E%s and c_name=E%s"""
            self.cursor.execute(sql, (item['stock_code'], item['c_name']))
            result = self.cursor.fetchone()
            if result is None:
                # 插入
                item['english_name'] = item['english_name'].replace("'", r"''").replace('"', r'\"')
                item['main_business'] = item['main_business'].replace("'", r"''").replace('"', r'\"')
                item['c_history'] = item['c_history'].replace("'", r"''").replace('"', r'\"')
                item['business_scope'] = item['business_scope'].replace("'", r"''").replace('"', r'\"')

                sql = """insert into t_exponent_company_profile (stock_code, c_name, org, address, chinese_f_short, address_detail, company_name, tel, english_name, mail, r_capital, chairman, staff_num, secretary_name, legal_man, secretary_tel, boss, secretary_fax, c_inter, secretary_mail, main_business, business_scope, c_history, d_e_o, m_d) values(E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s')""" \
                      % (item['stock_code'], item['c_name'], item['org'], item['address'], item['chinese_f_short'],
                         item['address_detail'], item['company_name'], item['tel'], item['english_name'], item['mail'],
                         item['r_capital'], item['chairman'], item['staff_num'], item['secretary_name'],
                         item['legal_man'],
                         item['secretary_tel'], item['boss'], item['secretary_fax'], item['c_inter'],
                         item['secretary_mail'],
                         item['main_business'], item['business_scope'], item['c_history'], item['d_o_e'], item['m_d'])
                try:
                    self.cursor.execute(sql)
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    logging.debug(item['stock_code'], e)
                    self.conn.rollback()
            else:
                sql = 'update t_exponent_company_profile set c_name=%s, org=%s, address=%s, chinese_f_short=%s, address_detail=%s, company_name=%s, tel=%s, english_name=%s, mail=%s, r_capital=%s, chairman=%s, staff_num=%s, secretary_name=%s, legal_man=%s, secretary_tel=%s, boss=%s, secretary_fax=%s, c_inter=%s, secretary_mail=%s, main_business=%s, business_scope=%s, c_history=%s, d_e_o=%s, m_d=%s WHERE stock_code=%s '
                try:
                    self.cursor.execute(sql, (item['c_name'], item['org'], item['address'], item['chinese_f_short'],
                                              item['address_detail'], item['company_name'], item['tel'],
                                              item['english_name'], item['mail'],
                                              item['r_capital'], item['chairman'], item['staff_num'],
                                              item['secretary_name'],
                                              item['legal_man'],
                                              item['secretary_tel'], item['boss'], item['secretary_fax'],
                                              item['c_inter'],
                                              item['secretary_mail'],
                                              item['main_business'], item['business_scope'], item['c_history'],
                                              item['d_o_e'], item['m_d'], item['stock_code']))
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    logging.debug(item['stock_code'], e)
                    self.conn.rollback()

        elif isinstance(item, Company_IPO_Item):
            # 查看
            sql = """select stock_code from t_exponent_company_ipo where stock_code=E%s and c_name=E%s"""
            self.cursor.execute(sql, (item['stock_code'], item['c_name']))
            result = self.cursor.fetchone()
            if result is None:
                sql = "insert into t_exponent_company_ipo (stock_code, c_name, d_o_e, m_d, dis, unit_price, issue_num, issue_price, total_price, issuance_fee, issue_winning, issue_pe, per_share, net_asset, opening_price, closing_price, turnover_rate, consignee, referrer, law_office) values(E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s')" \
                      % (
                          item['stock_code'], item['c_name'], item['d_o_e'], item['m_d'], item['dis'],
                          item['unit_price'],
                          item['issue_num'], item['issue_price'], item['total_price'], item['issuance_fee'],
                          item['issue_winning'], item['issue_pe'], item['per_share'], item['net_asset'],
                          item['opening_price'],
                          item['closing_price'], item['turnover_rate'], item['consignee'], item['referrer'],
                          item['law_office'])
                try:
                    self.cursor.execute(sql)
                    self.conn.commit()
                except Exception as e:
                    print(e, item)
                    self.conn.rollback()
            else:
                sql = """update t_exponent_company_ipo set c_name=%s, d_o_e=%s, m_d=%s, dis=%s, unit_price=%s, issue_num=%s, issue_price=%s, total_price=%s, issuance_fee=%s, issue_winning=%s, issue_pe=%s, per_share=%s, net_asset=%s, opening_price=%s, closing_price=%s, turnover_rate=%s, consignee=%s, referrer=%s, law_office=%s WHERE stock_code=%s """
                try:
                    self.cursor.execute(sql,
                                        (item['c_name'], item['d_o_e'], item['m_d'], item['dis'], item['unit_price'],
                                         item['issue_num'], item['issue_price'], item['total_price'],
                                         item['issuance_fee'],
                                         item['issue_winning'], item['issue_pe'], item['per_share'], item['net_asset'],
                                         item['opening_price'],
                                         item['closing_price'], item['turnover_rate'], item['consignee'],
                                         item['referrer'],
                                         item['law_office'], item['stock_code']))
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    self.conn.rollback()

        elif isinstance(item, Company_Income_Item):
            # 查看
            sql = """select stock_code from t_exponent_company_type_income where uid=%s and stock_code=%s """
            self.cursor.execute(sql, (
                (item['stock_code'] + item['classify'] + item['type_name'] + item['income']), item['stock_code']))
            result = self.cursor.fetchone()
            if result is None:
                sql = "insert into t_exponent_company_type_income (uid, stock_code, c_name, classify, type_name, income, cost, profit, profit_rate, profits_account, report_date) values(E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s')" \
                      % (
                          (item['stock_code'] + item['classify'] + item['type_name'] + item['income']),
                          item['stock_code'],
                          item['c_name'], item['classify'], item['type_name'], item['income'], item['cost'],
                          item['profit'], item['profit_rate'], item['profits_account'], item['report_date'])
                try:
                    self.cursor.execute(sql)
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    self.conn.rollback()
            else:
                sql = """update t_exponent_company_type_income set c_name=%s, cost=%s, profit=%s, profit_rate=%s, profits_account=%s, report_date=%s WHERE uid=%s """
                try:
                    self.cursor.execute(sql, (item['c_name'], item['cost'], item['profit'], item['profit_rate'],
                                              item['profits_account'], item['report_date'], (
                                                      item['stock_code'] + item['classify'] + item['type_name'] +
                                                      item['income'])))
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    self.conn.rollback()
        self.conn.close()
        return item


# sit_exponent
class Sit_Psycopg2pipline(object):
    conn = None
    cursor = None

    def process_item(self, item, spider):
        self.conn = psycopg2.connect(
            database=database_info_target_sit["database"],
            user=database_info_target_sit["user"],
            password=database_info_target_sit["password"],
            host=database_info_target_sit["host"],
            port=database_info_target_sit["port"]
        )

        self.cursor = self.conn.cursor()
        if isinstance(item, Company_Profile_Item):
            # 查看
            sql = """select stock_code from t_exponent_company_profile where stock_code=E%s and c_name=E%s"""
            self.cursor.execute(sql, (item['stock_code'], item['c_name']))
            result = self.cursor.fetchone()
            if result is None:
                # 插入
                item['english_name'] = item['english_name'].replace("'", r"''").replace('"', r'\"')
                item['main_business'] = item['main_business'].replace("'", r"''").replace('"', r'\"')
                item['c_history'] = item['c_history'].replace("'", r"''").replace('"', r'\"')
                item['business_scope'] = item['business_scope'].replace("'", r"''").replace('"', r'\"')

                sql = """insert into t_exponent_company_profile (stock_code, c_name, org, address, chinese_f_short, address_detail, company_name, tel, english_name, mail, r_capital, chairman, staff_num, secretary_name, legal_man, secretary_tel, boss, secretary_fax, c_inter, secretary_mail, main_business, business_scope, c_history, d_e_o, m_d) values(E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s')""" \
                      % (item['stock_code'], item['c_name'], item['org'], item['address'], item['chinese_f_short'],
                         item['address_detail'], item['company_name'], item['tel'], item['english_name'], item['mail'],
                         item['r_capital'], item['chairman'], item['staff_num'], item['secretary_name'],
                         item['legal_man'],
                         item['secretary_tel'], item['boss'], item['secretary_fax'], item['c_inter'],
                         item['secretary_mail'],
                         item['main_business'], item['business_scope'], item['c_history'], item['d_o_e'], item['m_d'])
                try:
                    self.cursor.execute(sql)
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    logging.debug(item['stock_code'], e)
                    self.conn.rollback()
            else:
                sql = 'update t_exponent_company_profile set c_name=%s, org=%s, address=%s, chinese_f_short=%s, address_detail=%s, company_name=%s, tel=%s, english_name=%s, mail=%s, r_capital=%s, chairman=%s, staff_num=%s, secretary_name=%s, legal_man=%s, secretary_tel=%s, boss=%s, secretary_fax=%s, c_inter=%s, secretary_mail=%s, main_business=%s, business_scope=%s, c_history=%s, d_e_o=%s, m_d=%s WHERE stock_code=%s '
                try:
                    self.cursor.execute(sql, (item['c_name'], item['org'], item['address'], item['chinese_f_short'],
                                              item['address_detail'], item['company_name'], item['tel'],
                                              item['english_name'], item['mail'],
                                              item['r_capital'], item['chairman'], item['staff_num'],
                                              item['secretary_name'],
                                              item['legal_man'],
                                              item['secretary_tel'], item['boss'], item['secretary_fax'],
                                              item['c_inter'],
                                              item['secretary_mail'],
                                              item['main_business'], item['business_scope'], item['c_history'],
                                              item['d_o_e'], item['m_d'], item['stock_code']))
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    logging.debug(item['stock_code'], e)
                    self.conn.rollback()
        self.conn.close()
        return item


# pro_exponent
class Pro_Psycopg2pipline(object):
    conn = None
    cursor = None

    def process_item(self, item, spider):
        self.conn = psycopg2.connect(
            database=database_info_target_pro["database"],
            user=database_info_target_pro["user"],
            password=database_info_target_pro["password"],
            host=database_info_target_pro["host"],
            port=database_info_target_pro["port"]
        )

        self.cursor = self.conn.cursor()
        if isinstance(item, Company_Profile_Item):
            # 查看
            sql = """select stock_code from t_exponent_company_profile where stock_code=E%s and c_name=E%s"""
            self.cursor.execute(sql, (item['stock_code'], item['c_name']))
            result = self.cursor.fetchone()
            if result is None:
                # 插入
                item['english_name'] = item['english_name'].replace("'", r"''").replace('"', r'\"')
                item['main_business'] = item['main_business'].replace("'", r"''").replace('"', r'\"')
                item['c_history'] = item['c_history'].replace("'", r"''").replace('"', r'\"')
                item['business_scope'] = item['business_scope'].replace("'", r"''").replace('"', r'\"')

                sql = """insert into t_exponent_company_profile (stock_code, c_name, org, address, chinese_f_short, address_detail, company_name, tel, english_name, mail, r_capital, chairman, staff_num, secretary_name, legal_man, secretary_tel, boss, secretary_fax, c_inter, secretary_mail, main_business, business_scope, c_history, d_e_o, m_d) values(E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s',E'%s')""" \
                      % (item['stock_code'], item['c_name'], item['org'], item['address'], item['chinese_f_short'],
                         item['address_detail'], item['company_name'], item['tel'], item['english_name'], item['mail'],
                         item['r_capital'], item['chairman'], item['staff_num'], item['secretary_name'],
                         item['legal_man'],
                         item['secretary_tel'], item['boss'], item['secretary_fax'], item['c_inter'],
                         item['secretary_mail'],
                         item['main_business'], item['business_scope'], item['c_history'], item['d_o_e'], item['m_d'])
                try:
                    self.cursor.execute(sql)
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    logging.debug(item['stock_code'], e)
                    self.conn.rollback()
            else:
                sql = 'update t_exponent_company_profile set c_name=%s, org=%s, address=%s, chinese_f_short=%s, address_detail=%s, company_name=%s, tel=%s, english_name=%s, mail=%s, r_capital=%s, chairman=%s, staff_num=%s, secretary_name=%s, legal_man=%s, secretary_tel=%s, boss=%s, secretary_fax=%s, c_inter=%s, secretary_mail=%s, main_business=%s, business_scope=%s, c_history=%s, d_e_o=%s, m_d=%s WHERE stock_code=%s '
                try:
                    self.cursor.execute(sql, (item['c_name'], item['org'], item['address'], item['chinese_f_short'],
                                              item['address_detail'], item['company_name'], item['tel'],
                                              item['english_name'], item['mail'],
                                              item['r_capital'], item['chairman'], item['staff_num'],
                                              item['secretary_name'],
                                              item['legal_man'],
                                              item['secretary_tel'], item['boss'], item['secretary_fax'],
                                              item['c_inter'],
                                              item['secretary_mail'],
                                              item['main_business'], item['business_scope'], item['c_history'],
                                              item['d_o_e'], item['m_d'], item['stock_code']))
                    self.conn.commit()
                except Exception as e:
                    print(e)
                    logging.debug(item['stock_code'], e)
                    self.conn.rollback()
        self.conn.close()
        return item


# 美股代码写入
class American_Stock_Pipline(object):
    now_date = ''
    conn_mysql = None

    def open_spider(self, spider):
        if spider.name == 'American_stock_info':
            self.conn_mysql = pymysql.Connect(
                database=database_rst_company["database"],
                user=database_rst_company["user"],
                password=database_rst_company["password"],
                host=database_rst_company["host"],
                port=database_rst_company["port"]
            )

    def close_spider(self, spider):
        if spider.name == 'American_stock_info':
            # 爬虫结束，同步今天数据
            save_to_postgresql_main(spider.name, self.now_date, self.conn_mysql, '')
        print('close_spider')
        self.conn_mysql.close()

    def process_item(self, item, spider):

        if spider.name == 'American_stock_info':
            self.conn_mysql.ping(reconnect=True)
            cursor = self.conn_mysql.cursor()
            self.now_date = item['end_date']

            # 更新postgresql股票代码
            update_stock_code_postgresql_main(item, 't_exponent_company_us')

            # 获取父类别
            parent_category = get_p_c(item['category'])
            if item['category'] == '':
                item['category'] = '综合'

            # 更新root分类表
            stock_classify_main(item, '美股')

            item['stock_name'] = pymysql.escape_string(item['stock_name'])
            item['english_name'] = pymysql.escape_string(item['english_name'])
            item['stock_code'] = pymysql.escape_string(item['stock_code'])

            # 更新mysql股票代码
            update_stock_code_mysql(item, 'allstocks_us')

            adjust = ''
            # 往前一天推算
            select_sql = """select * from allstock_market_info_us where stock_code=%s and date=%s"""
            cursor.execute(select_sql, (item['stock_code'], item['end_date']))
            if cursor.fetchone() is None:
                # 插入
                insert_sql = """insert into allstock_market_info_us (parent_category, category, date, c_name, english_name, stock_code, adjust, t_open, t_high, t_low, y_close, t_close, t_volume, rise_fall, applies, amplitude, market_value, ratio, listing) values('%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,'%s',%s,'%s','%s','%s','%s','%s','%s','%s')""" \
                             % (parent_category, item['category'], item['end_date'], item['stock_name'],
                                item['english_name'],
                                item['stock_code'], adjust, item['open_price'],
                                item['high_price'], item['low_price'], item['close_price'], item['latest_price'],
                                item['volume'], item['rise_fall'], item['applies'], item['amplitude'],
                                item['market_value'], item['ratio'], item['listing'])
                try:
                    cursor.execute(insert_sql)
                    self.conn_mysql.commit()
                except Exception as e:
                    print(e, item['stock_name'], item['stock_code'])
                    self.conn_mysql.rollback()
            else:
                update_sql = """update allstock_market_info_us set c_name=%s, english_name=%s, stock_code=%s, adjust=%s, t_open=%s, t_high=%s, t_low=%s, y_close=%s, t_close=%s, t_volume=%s, rise_fall=%s, applies=%s, amplitude=%s, market_value=%s, ratio=%s, listing=%s where stock_code=%s and date=%s"""
                try:
                    cursor.execute(update_sql, (
                        item['stock_name'], item['english_name'],
                        item['stock_code'], adjust, item['open_price'],
                        item['high_price'], item['low_price'], item['close_price'], item['latest_price'],
                        item['volume'], item['rise_fall'], item['applies'], item['amplitude'], item['market_value'],
                        item['ratio'], item['listing'], item['stock_code'], item['end_date']))
                    self.conn_mysql.commit()
                except Exception as e:
                    print(e, item['stock_name'], item['stock_code'])
                    self.conn_mysql.rollback()
        elif spider.name == 'US_ChinaStock_info':
            stock_classify_main(item, '中国概念股')
        else:
            pass
        return item


# 港股代码写入
class HongKong_Stock_Pipline(object):
    now_date = ''
    conn_mysql = None

    def open_spider(self, spider):
        if spider.name == 'hk_stock_info':
            self.conn_mysql = pymysql.Connect(
                database=database_rst_company["database"],
                user=database_rst_company["user"],
                password=database_rst_company["password"],
                host=database_rst_company["host"],
                port=database_rst_company["port"]
            )

    def close_spider(self, spider):
        if spider.name == 'hk_stock_info':
            # 爬虫结束，同步今天数据
            save_to_postgresql_main(spider.name, self.now_date, self.conn_mysql, '')
        else:
            pass
        print('close_spider')
        self.conn_mysql.close()

    def process_item(self, item, spider):
        if spider.name == 'hk_stock_info':
            self.conn_mysql.ping(reconnect=True)
            cursor = self.conn_mysql.cursor()
            self.now_date = item['end_date']

            # 更新postgresql股票代码
            update_stock_code_postgresql_main(item, 't_exponent_company_hk')

            item['stock_name'] = pymysql.escape_string(item['stock_name'])
            item['stock_code'] = pymysql.escape_string(item['stock_code'])

            # 更新mysql股票代码
            update_stock_code_mysql(item, 'allstocks_hk')

            # 未复权
            adjust = ''

            # 查
            select_sql = """select * from allstock_market_info_hk where stock_code=%s and date=%s"""
            cursor.execute(select_sql, (item['stock_code'], item['end_date']))
            if cursor.fetchone() is None:
                # 插入
                insert_sql = """insert into allstock_market_info_hk (stock_code, date, c_name, adjust, t_open, t_high, t_low, y_close, t_close, t_volume, t_volume_price, rise_fall, applies) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,'%s','%s')""" \
                             % (item['stock_code'], item['end_date'], item['stock_name'], adjust, item['open_price'],
                                item['high_price'], item['low_price'], item['close_price'], item['latest_price'],
                                item['volume_num'], item['volume_price'], item['rise_fall'], item['applies'])
                try:
                    cursor.execute(insert_sql)
                    self.conn_mysql.commit()
                except Exception as e:
                    print(e, item['stock_code'], item['stock_name'])
                    self.conn_mysql.rollback()
            else:
                update_sql = """update allstock_market_info_hk set c_name=%s, t_open=%s, t_high=%s, t_low=%s, y_close=%s, t_close=%s, t_volume=%s, t_volume_price=%s, rise_fall=%s, applies=%s where stock_code=%s and date=%s"""
                try:
                    cursor.execute(update_sql, (
                        item['stock_name'], item['open_price'], item['high_price'], item['low_price'],
                        item['close_price'],
                        item['latest_price'], item['volume_num'], item['volume_price'], item['rise_fall'],
                        item['applies'],
                        item['stock_code'], item['end_date']))
                    self.conn_mysql.commit()
                except Exception as e:
                    print(e, item['stock_name'], item['stock_code'])
                    self.conn_mysql.rollback()
        elif spider.name == 'hk_stock':
            stock_classify_main(item, '港股')
        else:
            pass
        return item


# 同步到postgresql接口
def save_to_postgresql_main(name, date, db_conn, data_type):
    data = real_mysql_table(name, date, db_conn, data_type)
    # print(data)
    if data == []:
        return
    save_to_postgresql(name, data, '232', data_type)
    save_to_postgresql(name, data, 'sit', data_type)
    save_to_postgresql(name, data, 'pro', data_type)


# 更新新上市股票接口
def update_stock_code_postgresql_main(item, table):
    update_stock_code_postgresql(item, table, '232')
    update_stock_code_postgresql(item, table, 'sit')
    update_stock_code_postgresql(item, table, 'pro')


# 分类root表更新接口
@async_func
def stock_classify_main(item, args_type):
    if args_type == '中国概念股':
        stock_classify('232', item['category'], '中国概念股', item['stock_name'], 't_exponent_company_us',
                       item['stock_code'])
        stock_classify('sit', item['category'], '中国概念股', item['stock_name'], 't_exponent_company_us',
                       item['stock_code'])
        stock_classify('pro', item['category'], '中国概念股', item['stock_name'], 't_exponent_company_us',
                       item['stock_code'])
    elif args_type == '港股':
        stock_classify('232', item['groups'], item['parent_groups'], item['stock_name'], 't_exponent_company_hk',
                       item['stock_code'])
        stock_classify('sit', item['groups'], item['parent_groups'], item['stock_name'], 't_exponent_company_hk',
                       item['stock_code'])
        stock_classify('pro', item['groups'], item['parent_groups'], item['stock_name'], 't_exponent_company_hk',
                       item['stock_code'])

    elif args_type == '美股':
        parent_category = get_p_c(item['category'])
        stock_classify('232', item['category'], parent_category, item['stock_name'], 't_exponent_company_us',
                       item['stock_code'])
        stock_classify('sit', item['category'], parent_category, item['stock_name'], 't_exponent_company_us',
                       item['stock_code'])
        stock_classify('pro', item['category'], parent_category, item['stock_name'], 't_exponent_company_us',
                       item['stock_code'])
    elif args_type == 'ETF':
        stock_classify('232', '综合', 'ETF', item['stock_name'], 't_exponent_company_daily_fund_sz',
                       item['stock_code'])
        stock_classify('sit', '综合', 'ETF', item['stock_name'], 't_exponent_company_daily_fund_sz',
                       item['stock_code'])
        stock_classify('pro', '综合', 'ETF', item['stock_name'], 't_exponent_company_daily_fund_sz',
                       item['stock_code'])
    else:
        print(f'未知的{args_type}')


if __name__ == '__main__':
    conn_mysql = pymysql.Connect(
        database=database_rst_company["database"],
        user=database_rst_company["user"],
        password=database_rst_company["password"],
        host=database_rst_company["host"],
        port=database_rst_company["port"]
    )


    cursor = conn_mysql.cursor()
    spider_name = 'ETF_market_info'
    # sql = """select distinct date from all_fund_market_info_sz where date between %s and %s"""
    # cursor.execute(sql, ('2020-06-19', '2020-08-20'))

    sql = """select distinct date from all_fund_investment_cgmx """
    # sql = """select distinct date from all_fund_investment_zchy"""

    cursor.execute(sql)
    result = cursor.fetchall()
    for date in result:
        print(date)
        # save_to_postgresql_main(spider_name, str(date[0]), conn_mysql, '')
        save_to_postgresql_main(spider_name, str(date[0]), conn_mysql, 'cgmx')
        # save_to_postgresql_main(spider_name, str(date[0]), conn_mysql, 'zchy')


    # # 每日数据

    # spider_date = '2020-08-20'
    # save_to_postgresql_main(spider_name, spider_date, conn_mysql, '')
