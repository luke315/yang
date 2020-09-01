# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/6/10 13:59
# software: PyCharm

"""
文件说明：
"""
import datetime

import pymysql
import psycopg2

from lib.db_args import database_info_target, database_info_target2

current_time = datetime.date.today()


'''
A：年
Q：季
M：月
W：周
D：日

1、根据时间频率读库
2、获取相关url
3、根据url和时间参数，调用增量爬虫
4、新数据更新到数据库
5、leaf目录级联跟新时间


'''
# 获取新增url
class mysql_setting:
    conn = None
    cursor = None

    def __init__(self):
        self.conn = psycopg2.connect(
                    database=database_info_target["database"],
                    user=database_info_target["user"],
                    password=database_info_target["password"],
                    host=database_info_target["host"],
                    port=database_info_target["port"])
    # 获取叶子节点url
    def read_sql(self, epx_frequency):
        self.cursor = self.conn.cursor()
        # 根据频率查询
        select_sql = """SELECT  ext2, code FROM t_exponent_eia WHERE ext4=%s and status=%s and ext3 = %s"""
        self.cursor.execute(select_sql, ('LEAF', 'valid', epx_frequency))

        # 频率倒序
        # select_sql = """SELECT ext2 FROM t_exponent_eia WHERE ext3=%s order by code desc """
        # self.cursor.execute(select_sql, epx_frequency)

        # 查询所有leaf节点
        # select_sql = """SELECT code, ext2 FROM t_exponent_eia WHERE ext4=%s and status=%s"""
        # self.cursor.execute(select_sql, ('LEAF', 'valid'))
        result = self.cursor.fetchall()
        self.conn.close()
        return result

    # 获取根节点url
    def read_sql1(self, num):
        self.cursor = self.conn.cursor()
        select_sql = """SELECT ext2 FROM t_exponent_eia WHERE ext4=%s and status=%s LIMIT 10000 OFFSET %s"""
        self.cursor.execute(select_sql, ('NODE', 'valid', num))
        result = self.cursor.fetchall()
        self.conn.close()
        return result

    def update_sql(self, name):
        self.cursor = self.conn.cursor()
        # 去掉几大洲
        # sql = """update t_exponent_eia set status=%s WHERE reverse(desc_s) like reverse(%s)"""
        sql = """update t_exponent_eia set status=%s WHERE ext4=%s and status=%s and desc_s like %s"""

        # East South West North
        try:
            self.cursor.execute(sql, ('invalid', 'LEAF', 'valid', '%s%s%s' % ('%', name, '%')))
            self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()
        # select_sql = """SELECT desc_s FROM t_exponent_eia WHERE status=%s and reverse(desc_s) like reverse(%s)"""
        # self.cursor.execute(select_sql, ('valid', '%s%s%s' % ('%', name, '%')))
        # result = self.cursor.fetchall()
        # print(result)
        self.conn.close()

def read_excel():
    import xlrd
    data = xlrd.open_workbook('C:/Users/EDZ/Desktop/美国州.xlsx')
    # 通过文件名获得工作表,获取工作表1
    table = data.sheet_by_name('Sheet3')
    table_data = table.col_values(0)
    print(table_data)
    return table_data


if __name__ == '__main__':
    # res = mysql_setting()
    # for i in res.read_sql('W', 1):
    #     url = eval(i[0]).get('id')
    #     print(url)

    # data_list_us = read_excel()
    # for i in data_list_us:
    #     print(i)
    #     mysql_setting().update_sql(i)

    # 单独去除
    mysql_setting().update_sql('Virgin Islands')
