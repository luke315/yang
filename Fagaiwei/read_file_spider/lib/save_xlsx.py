# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/6/4 17:52
# software: PyCharm

"""
文件说明：
"""
import os

import xlrd
from xlutils.copy import copy

import xlwt  #写excel，导入模块
from read_file_spider.lib.lib_args import xlsx_args

# 单条直接存
def write_excel_xlsx(book_name_xlsx, sheet_name_xlsx, value):
    if not os.path.exists(book_name_xlsx):
        workbook = xlwt.Workbook()
        sheet = workbook.add_sheet(sheet_name_xlsx)
        for j in range(0, len(xlsx_args)):
            sheet.write(0, j, str(xlsx_args[j]))
        for j in range(0, len(value)):
            sheet.write(1, j, str(value[j]))
        workbook.save(book_name_xlsx)
    else:
        wt = xlrd.open_workbook(book_name_xlsx, formatting_info=True)
        # 复制文件进行操作，直接操作原文件会覆盖
        new_wt = copy(wt)
        new_sheet = new_wt.get_sheet(0)
        table = wt.sheet_by_name(sheet_name_xlsx)
        rows_num = table.nrows
        for num in range(len(value)):
            new_sheet.write(rows_num, num, value[num])
        new_wt.save(book_name_xlsx)

