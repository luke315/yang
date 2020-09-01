# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ProItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class File_Item(scrapy.Item):
    stock_code = scrapy.Field()
    company_name = scrapy.Field()
    pdf_file = scrapy.Field()
    html_file = scrapy.Field()
    file_name = scrapy.Field()
    file_type = scrapy.Field()
    file_date = scrapy.Field()


class Company_Profile_Item(scrapy.Item):
    stock_code = scrapy.Field()
    c_name = scrapy.Field()
    org = scrapy.Field()
    address = scrapy.Field()
    chinese_f_short = scrapy.Field()
    address_detail = scrapy.Field()
    company_name = scrapy.Field()
    tel = scrapy.Field()
    english_name = scrapy.Field()
    mail = scrapy.Field()
    r_capital = scrapy.Field()
    chairman = scrapy.Field()
    staff_num = scrapy.Field()
    secretary_name = scrapy.Field()
    legal_man = scrapy.Field()  # 法人代表
    secretary_tel = scrapy.Field()
    boss = scrapy.Field()
    secretary_fax = scrapy.Field()
    c_inter = scrapy.Field()
    secretary_mail = scrapy.Field()
    main_business = scrapy.Field()
    business_scope = scrapy.Field()
    c_history = scrapy.Field()
    d_o_e = scrapy.Field()
    m_d = scrapy.Field()


class Company_IPO_Item(scrapy.Item):
    stock_code = scrapy.Field()
    c_name = scrapy.Field()
    d_o_e = scrapy.Field()
    m_d = scrapy.Field()
    dis = scrapy.Field()
    unit_price = scrapy.Field()
    issue_num = scrapy.Field()
    issue_price = scrapy.Field()
    total_price = scrapy.Field()
    issuance_fee = scrapy.Field()
    issue_winning = scrapy.Field()
    issue_pe = scrapy.Field()
    per_share = scrapy.Field()
    net_asset = scrapy.Field()
    opening_price = scrapy.Field()
    closing_price = scrapy.Field()
    turnover_rate = scrapy.Field()
    consignee = scrapy.Field()
    referrer = scrapy.Field()
    law_office = scrapy.Field()


class Company_Income_Item(scrapy.Item):
    stock_code = scrapy.Field()
    c_name = scrapy.Field()
    classify = scrapy.Field()
    type_name = scrapy.Field()
    income = scrapy.Field()
    cost = scrapy.Field()
    profit = scrapy.Field()
    profit_rate = scrapy.Field()
    profits_account = scrapy.Field()
    report_date = scrapy.Field()


class American_Stock_Item(scrapy.Item):
    stock_code = scrapy.Field()
    english_name = scrapy.Field()  # 英文名
    category = scrapy.Field()  # 分类
    category_id = scrapy.Field()  # 父id
    stock_name = scrapy.Field()
    latest_price = scrapy.Field()  # 最新价
    rise_fall = scrapy.Field()  # 涨跌额
    applies = scrapy.Field()  # 涨跌幅
    amplitude = scrapy.Field()  # 振幅
    close_price = scrapy.Field()  # 昨收/开盘
    open_price = scrapy.Field()  # 昨收/开盘
    low_price = scrapy.Field()  # 最高/最低价
    high_price = scrapy.Field()  # 最高/最低价
    volume = scrapy.Field()  # 成交量
    market_value = scrapy.Field()  # 市值
    ratio = scrapy.Field()  # 市盈率
    groups = scrapy.Field()  # 行业板块
    listing = scrapy.Field()  # 上市地
    end_date = scrapy.Field()


class HongKong_Stock_Item(scrapy.Item):
    stock_code = scrapy.Field()
    stock_name = scrapy.Field()
    latest_price = scrapy.Field()  # 最新价
    rise_fall = scrapy.Field()  # 涨跌额
    applies = scrapy.Field()  # 涨跌幅
    # amplitude = scrapy.Field()  # 振幅
    close_price = scrapy.Field()  # 昨收
    open_price = scrapy.Field()  # 今开
    high_price = scrapy.Field()  # 最高
    low_price = scrapy.Field()  # 最低
    # lowest_price = scrapy.Field()  # 最高/最低价
    volume_num = scrapy.Field()  # 成交量
    volume_price = scrapy.Field()  # 成交额
    # market_value = scrapy.Field()  # 市值
    # ratio = scrapy.Field()  # 市盈率
    # groups = scrapy.Field()  # 行业板块
    # listing = scrapy.Field()  # 上市地
    end_date = scrapy.Field()
    groups = scrapy.Field()
    parent_groups = scrapy.Field()


class ETF_Item(scrapy.Item):
    fund_date = scrapy.Field()  # 日期
    stock_code = scrapy.Field()
    stock_name = scrapy.Field()
    fund_sname = scrapy.Field()
    fund_high = scrapy.Field()  # 最高价
    fund_low = scrapy.Field()  # 最低价
    fund_open = scrapy.Field()  # 今开
    fund_close = scrapy.Field()  # 最新价
    fund_y_close = scrapy.Field()  # 昨收
    fund_up_down_percent = scrapy.Field()  # 涨跌幅
    fund_up_down = scrapy.Field()  # 涨跌额
    fund_volume_price = scrapy.Field()  # 成交额
    fund_volume_num = scrapy.Field()  # 成交量
    fund_zzc = scrapy.Field()  # 总资产
    fund_zfe = scrapy.Field()  # 总份额
    fund_hsl = scrapy.Field()  # 换手率
    fund_zjl = scrapy.Field()  # 折价率
    fund_dwjz = scrapy.Field()  # 单位净值
    fund_ljjz = scrapy.Field()  # 累计净值
    fund_zzl = scrapy.Field()  # 增长率

    fund_jzzs_time = scrapy.Field()  # 临时字段
    fund_zyjl_time = scrapy.Field()  # 临时字段
    fund_y_hsl = scrapy.Field()  # 临时字段
    fund_y_zjl = scrapy.Field()  # 临时字段


    report_date = scrapy.Field()  # 临时字段
    cgmx_code_name = scrapy.Field()
    cgmx_code = scrapy.Field()
    cgmx_account = scrapy.Field()
    cgmx_hold = scrapy.Field()
    cgmx_value = scrapy.Field()

    type_cgmx_or_zchy = scrapy.Field()

    zchy_rank = scrapy.Field()
    zchy_form = scrapy.Field()
    zchy_value = scrapy.Field()
    zchy_account = scrapy.Field()
    zchy_account_change = scrapy.Field()