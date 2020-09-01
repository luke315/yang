# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/6/2 15:47
# software: PyCharm

"""
文件说明：
"""
from threading import Thread
import psycopg2
from lib.db_args import database_info_target2, database_info_target
from lib.md5 import md5_sql


class eia_value_sql_setting:
    conn = None
    cursor = None

    def __init__(self):
        self.conn = psycopg2.connect(
            database=database_info_target2["database"],
            user=database_info_target2["user"],
            password=database_info_target2["password"],
            host=database_info_target2["host"],
            port=database_info_target2["port"])


class eia_epx_sql_setting:
    conn = None
    cursor = None

    def __init__(self):
        self.conn = psycopg2.connect(
            database=database_info_target["database"],
            user=database_info_target["user"],
            password=database_info_target["password"],
            host=database_info_target["host"],
            port=database_info_target["port"])

# 异步调用
def async(f):
    def wrapper(*args, **kwargs):
        thr = Thread(target=f, args=args, kwargs=kwargs)
        thr.start()

    return wrapper


def check_eia_value(code):
    conn = psycopg2.connect(
        database=database_info_target2["database"],
        user=database_info_target2["user"],
        password=database_info_target2["password"],
        host=database_info_target2["host"],
        port=database_info_target2["port"])
    cursor = conn.cursor()
    select_sql = f"""SELECT * FROM t_exponent_value_eia WHERE exponent_code=%s and status=%s"""
    cursor.execute(select_sql, (code, 'valid'))
    result = cursor.fetchone()
    conn.close()
    if result == None:
        return True
    else:
        return False


# 获取指标url节点
def check_eia_epx():
    conn = psycopg2.connect(
        database=database_info_target["database"],
        user=database_info_target["user"],
        password=database_info_target["password"],
        host=database_info_target["host"],
        port=database_info_target["port"])

    cursor = conn.cursor()
    # 查询所有leaf节点
    # select_sql = """SELECT ext2, code FROM t_exponent_eia WHERE ext4=%s and status=%s order by code desc LIMIT 10000 OFFSET %s"""
    # cursor.execute(select_sql, ('LEAF', 'valid', num))

    select_sql = """SELECT ext2, code FROM t_exponent_eia WHERE ext4=%s and status=%s order by create_time desc"""
    cursor.execute(select_sql, ('LEAF', 'valid'))

    result = cursor.fetchall()
    conn.close()
    return result


def main():
    import redis
    conn = redis.Redis(host='127.0.0.1', port=6379, db=15)
    base_url = 'https://www.eia.gov/opendata/qb.php?'
    mysql_result = check_eia_epx()
    print(len(mysql_result))
    num = 1
    for i in range(0, len(mysql_result)-1):
        print(num)
        leaf_url = eval(mysql_result[i][0]).get('id')
        # 如果为空节点，则加入redis
        if check_eia_value(md5_sql(leaf_url)):
            url = base_url + leaf_url
            conn.lpush(f"eia_redis:start_urls", url)
        num += 1


if __name__ == '__main__':
    main()
