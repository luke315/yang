

# 分类
import datetime
import logging

import psycopg2

from lib.args.sql_args import database_info_target, database_info_target_sit, database_info_target_pro


def get_p_c(data):
    for k, v in category_data_us.items():
        if data in v:
            parent_category = k
            break
        else:
            parent_category = '其他'
    return parent_category


# 分类list
category_data_us = {'基础材料': ['采矿', '钢铁', '化学制品', '林产品/造纸'],
                    '科技': ['计算机', '半导体', '软件', '办公/企业设备', '计算机硬件'],
                    '工业': ['电力组件与设备', '运输', '电子', '多元化制造商', '航空航天/国防', '包装容器', '建筑材料', '机械-建筑-矿业', '金属构造/部件', '工程建设',
                           '机械-多元', '环境控制', '手工/机器工具', '运输及出租', '造船'],
                    '非周期性消费品': ['制药', '商业服务', '食品', '生物技术', '化妆品/护理', '医疗保健产品', '饮料', '健康护理服务', '农业', '家庭产品/用品'],
                    '公共事业': ['电力', '水业', '天然气'],
                    '金融': ['保险', '多元化金融服务', '银行', '房地产', '房地产信托投资', '私募股权投资', '储蓄及存款', '投资公司'],
                    '通信': ['电信', '广告', '互联网', '媒体', '媒体内容'],
                    '周期性消费品': ['零售', '娱乐', '汽车部件与设备', '航空公司', '房屋建筑商', '家具家饰', '服饰', '汽车制造', '住宿', '休闲', '销售/批发', '纺织品',
                               '玩具/游戏/爱好', '办公家具', '家用器具', '存储/仓储'],
                    '能源': ['油气服务', '油气', '能源-替代能源', '管道', '煤炭'],
                    '其他行业': ['综合']}

category_data_hk = {'恒生行业': []}

category_data_us_china = {'中国概念股': []}


# 哈希
import hashlib


def md5_sql(data, str_type):
    data_value = f'{data}us_and_hk{str_type}'
    res_value = hashlib.md5(data_value.encode(encoding='UTF-8')).hexdigest()
    return res_value


# 股票分类
def stock_classify(db, category, parent_category, c_name, table, stock_code):
    if db == '232':
        conn = psycopg2.connect(
            database=database_info_target["database"],
            user=database_info_target["user"],
            password=database_info_target["password"],
            host=database_info_target["host"],
            port=database_info_target["port"]
        )
    elif db == 'sit':
        conn = psycopg2.connect(
            database=database_info_target_sit["database"],
            user=database_info_target_sit["user"],
            password=database_info_target_sit["password"],
            host=database_info_target_sit["host"],
            port=database_info_target_sit["port"]
        )
    else:
        conn = psycopg2.connect(
            database=database_info_target_pro["database"],
            user=database_info_target_pro["user"],
            password=database_info_target_pro["password"],
            host=database_info_target_pro["host"],
            port=database_info_target_pro["port"]
        )

    top = 'ssgscd06-3d77-4efb-a78d-ad9a9cea3d80'
    if table == 't_exponent_company_us':
        if parent_category == '中国概念股':
            str_type = '中概股'
            save_root(conn, md5_sql('中国概念股', str_type), top, '中概股', table, '')
        else:
            str_type = '美股'
            save_root(conn, md5_sql('美国股市', str_type), top, '美股', table, '')
            for k, v in category_data_us.items():
                # 主节点目录
                save_root(conn, md5_sql(k, str_type), md5_sql('美国股市', str_type), k, table, '')
    elif table == 't_exponent_company_hk':
        str_type = '港股'
        save_root(conn, md5_sql('恒生行业', str_type), top, '港股', table, '')
    elif table == 't_exponent_company_daily_fund_sz':
        str_type = 'ETF'

        save_root(conn, md5_sql('ETF', str_type), top, 'ETF', table, '')

        save_root(conn, md5_sql(md5_sql(stock_code, str_type), md5_sql('ETF', str_type)),
                  md5_sql('ETF', str_type), c_name, table, stock_code)
        return
    else:
        print(f'未知table:{table}')
        return
    # 父级
    save_root(conn, md5_sql(category, str_type), md5_sql(parent_category, str_type), category, table, '')
    # 子级
    save_root(conn, md5_sql(md5_sql(stock_code, str_type), md5_sql(category, str_type)), md5_sql(category, str_type), c_name, table, stock_code)
    conn.close()

# 股票分类写入root表
def save_root(conn, code, parent_code, exponent_name, ref_table, ref_exponent_code):
    table = 't_exponent_root'
    if ref_table == 't_exponent_company_us':
        res1 = '美股'
    elif ref_table == 't_exponent_company_hk':
        res1 = '港股'
    elif ref_table == 't_exponent_company_daily_fund_sz':
        res1 = 'ETF'
    else:
        logging.error(f'未知{ref_table}')
        return
    if ref_exponent_code == '':
        res2 = '上市公司分类'
        ref_table = ''
    else:
        res2 = '上市公司'
    ext1 = res1 + res2
    cursor = conn.cursor()
    exponent_name = exponent_name.replace("'", "")
    select_sql = f"""select * from {table} where code=%s and parent_code=%s"""
    cursor.execute(select_sql, (code, parent_code))
    result = cursor.fetchone()
    if result is None:
        # 插入
        insert_sql = f"""insert into {table} (code, parent_code, exponent_name, ref_table, ref_exponent_code, ext1, status) values('%s','%s','%s','%s','%s','%s','%s')""" \
                     % (
                         code, parent_code, exponent_name, ref_table,
                         ref_exponent_code, ext1, 'valid')
        try:
            cursor.execute(insert_sql)
            conn.commit()
        except Exception as e:
            print(e, code, parent_code)
            conn.rollback()
    else:
        update_sql = f"""update {table} set exponent_name=%s, ref_table=%s, ref_exponent_code=%s, ext1=%s, status=%s, update_time=%s where code=%s and parent_code=%s"""
        try:
            cursor.execute(update_sql, (
            exponent_name, ref_table, ref_exponent_code, ext1, 'valid',
            str(datetime.datetime.now()), code, parent_code))
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()