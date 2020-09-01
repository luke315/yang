# eia分類
import psycopg2

from lib.db_args import database_info_target_sit


def eia_classify():
    star = 'star_node_page'
    conn = psycopg2.connect(
        database=database_info_target_sit['database'],
        user=database_info_target_sit['user'],
        password=database_info_target_sit['password'],
        host=database_info_target_sit['host'],
        port=database_info_target_sit['port'])
    cursor = conn.cursor()
    select_sql = f"""SELECT code, exponent_name FROM t_exponent_eia WHERE status=%s and ext2 like %s"""
    cursor.execute(select_sql, ('valid', '%s%s%s' % ('%', star, '%')))
    result = cursor.fetchall()
    for i in result:
        insert_sql = f"""insert into t_exponent_root (code, parent_code, exponent_name, ref_table, ref_exponent_code, status) values('%s','%s','%s','%s','%s','%s')""" \
                     % (
                         i[0], '7f0d0402-5c36-11ea-aa21-00d8612dnyf2', i[1], 't_exponent_eia', i[0], 'valid')
        try:
            cursor.execute(insert_sql)
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()


if __name__ == '__main__':
    eia_classify()