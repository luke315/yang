# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/6/11 17:53
# software: PyCharm

"""
文件说明：
"""
import hashlib

def md5_sql(data):
    res_value = hashlib.md5(data.encode(encoding='UTF-8')).hexdigest()
    return res_value


if __name__ == '__main__':
    md5_sql('hdslkhdfiulhilg')
    print(md5_sql('hdslkhdfiulhilg'))