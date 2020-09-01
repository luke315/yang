# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/6/4 16:19
# software: PyCharm

"""
文件说明：
"""
import pymysql


host = '47.100.162.232'
port = 3306
user = 'root'
password = '1qaz@WSX'
db = 'rst-company'


class sql_set:
    conn = None
    cursor = None

    def __init__(self):
        self.conn = pymysql.Connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db)

    def read_sql(self):
        self.cursor = self.conn.cursor()
        # select_sql = 'SELECT share_name, share_code FROM allstocks'
        select_sql = 'select share_name, share_code from allstocks order by share_code desc'
        self.cursor.execute(select_sql)
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


# 读取html文件
class mysql_setting:
    conn = None
    cursor = None

    def __init__(self):
        self.conn = pymysql.Connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db)

    def read_sql(self):
        self.cursor = self.conn.cursor()
        # select_sql = 'SELECT share_name, share_code FROM allstocks'
        select_sql = 'select stock_code, company, html_file  from stocks_file order by html_file desc'
        self.cursor.execute(select_sql)
        result = self.cursor.fetchall()
        self.conn.close()
        return result
