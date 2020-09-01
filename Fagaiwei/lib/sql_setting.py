# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/6/4 16:19
# software: PyCharm

"""
文件说明：
"""
import pymysql
import datetime

from lib.args.sql_args import database_rst_company

today_date = datetime.date.today()
end_time = datetime.timedelta(days=1)

class sql_set:
    conn = None
    cursor = None

    def __init__(self):
        self.conn = pymysql.Connect(
            database=database_rst_company["database"],
            user=database_rst_company["user"],
            password=database_rst_company["password"],
            host=database_rst_company["host"],
            port=database_rst_company["port"]
        )

    def read_sql(self):
        self.cursor = self.conn.cursor()
        # 全量
        # select_sql = 'SELECT share_name, share_code FROM allstocks'
        # self.cursor.execute(select_sql)

        # 增量
        select_sql = 'select share_name, share_code from allstocks where update_date between %s and %s'
        self.cursor.execute(select_sql, ('20200725', '20200807'))

        # # 增量
        # select_sql = 'select share_name, share_code from allstocks where update_date between %s and %s'
        # self.cursor.execute(select_sql, (str(today_date - end_time).replace('-', ''), str(today_date).replace('-', '')))

        result = self.cursor.fetchall()
        self.conn.close()
        return result

    def insert_sql(self, stock_code, company_name, pdf_file, html_file):
        self.cursor = self.conn.cursor()
        sql = 'insert into stocks_file (stock_code, company, pdf_file, html_file) values("%s","%s","%s","%s")' \
              % (stock_code, company_name, pdf_file, html_file)
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()
            self.conn.close()

    def update_sql(self):
        pass

    # 获取沪深基金代码
    def read_fund_code(self):
        self.cursor = self.conn.cursor()
        select_sql = 'select stock_code, c_name, s_name from all_funds_sz'
        self.cursor.execute(select_sql)
        result = self.cursor.fetchall()
        self.conn.close()
        return result

# 读取html文件
class mysql_setting:
    conn = None
    cursor = None

    def __init__(self):
        self.conn = pymysql.Connect(
            database=database_rst_company["database"],
            user=database_rst_company["user"],
            password=database_rst_company["password"],
            host=database_rst_company["host"],
            port=database_rst_company["port"]
        )
    def read_sql(self):
        self.cursor = self.conn.cursor()
        # select_sql = 'SELECT share_name, share_code FROM allstocks'
        select_sql = 'select stock_code, company, html_file  from stocks_file order by html_file desc'
        self.cursor.execute(select_sql)
        result = self.cursor.fetchall()
        self.conn.close()
        return result
