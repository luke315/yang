# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/6/8 10:39
# software: PyCharm

"""
文件说明：
"""
import re

from read_file_spider.lib.lib_args import words, book_name_xlsx, sheet_name_xlsx
from read_file_spider.lib.save_xlsx import write_excel_xlsx
from read_file_spider.lib.sql_setting import mysql_setting


class Html_Parse:

    def read_file(self):
        res = mysql_setting().read_sql()
        for stock_code, company, html_file in res:
            if html_file == '':
                pass
            else:
                content = self.open_file(html_file)
                file_name = html_file.split('HTML/')[1].split('.html')[0]
                self.word_count(file_name, stock_code, content)

    def open_file(self, html_file):
        with open(r'%s' % html_file, 'r', encoding='GBK', errors='ignore') as f:
            conten = f.read()
            f.close()
            return conten

    def word_count(self, file_name, stock_code, new_str):
        list_xlsx = []
        list_xlsx.append(stock_code)
        list_xlsx.append(file_name)
        for i in words.split('\t'):
            count = len(re.findall('%s' % i, new_str))
            list_xlsx.append(count)
        write_excel_xlsx(book_name_xlsx, sheet_name_xlsx, list_xlsx)

    
if __name__ == '__main__':
    res = Html_Parse()
    res.read_file()