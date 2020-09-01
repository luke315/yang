# coding=utf-8
import psycopg2
import pymysql


def create_postgresql_exponent():
    database_info_target = {
        "database": "rst_exponent",
        "user": "exponent",
        "password": "exponent",
        "host": "47.100.162.232",
        "port": "5432"
    }

    conn = psycopg2.connect(
        database=database_info_target["database"],
        user=database_info_target["user"],
        password=database_info_target["password"],
        host=database_info_target["host"],
        port=database_info_target["port"]
    )

    cur = conn.cursor()
    sql_ipo = 'CREATE TABLE t_exponent_company_ipo (stock_code varchar,c_name varchar,d_o_e varchar,m_d varchar,dis varchar,unit_price varchar,issue_num varchar,issue_price varchar,total_price varchar,issuance_fee varchar,issue_winning varchar,issue_pe varchar,per_share varchar,net_asset varchar, opening_price varchar,closing_price varchar,turnover_rate varchar,consignee varchar,referrer varchar,law_office varchar);'
    sql_profile = 'CREATE TABLE t_exponent_company_profile (stock_code varchar, c_name varchar, org varchar, address varchar, chinese_f_short varchar, address_detail varchar, company_name varchar, tel varchar, english_name varchar, mail varchar, r_capital varchar, chairman varchar, staff_num varchar, secretary_name varchar, legal_man varchar, secretary_tel varchar, boss varchar, secretary_fax varchar, c_inter varchar, secretary_mail varchar, main_business varchar, business_scope varchar, c_history varchar);'
    sql_income = 'CREATE TABLE t_exponent_company_type_income (uid varchar,stock_code varchar,c_name varchar,classify varchar,type_name varchar,income varchar,cost varchar,profit varchar,profits_account varchar,report_date varchar);'

    try:
        cur.execute(sql_ipo)
        cur.execute(sql_profile)
        cur.execute(sql_income)
    except:
        print("I can't drop our test database!")
    conn.commit()
    cur.close()


def create_mysql_stock_company():
    host = '47.100.162.232'
    port = 3306
    user = 'root'
    password = '1qaz@WSX'
    db = 'rst-company'

    conn = pymysql.Connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=db)

    cur = conn.cursor()
    # sql1 = """CREATE TABLE allstocks_us (stock_code varchar(32),c_name varchar(32),update_time date);"""
    sql2 = """CREATE TABLE allstocks_hk (stock_code varchar(32),c_name varchar(32),update_time date);"""
    try:
        # cur.execute(sql1)
        cur.execute(sql2)
    except Exception as e:
        print(e)
    conn.commit()
    conn.close()

# if __name__ == '__main__':
#     create_mysql_stock_company()