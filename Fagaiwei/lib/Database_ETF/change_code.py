
# node 列表
from lib.Database_ETF.save_exponent import save_exponent_sql


list_node = ['t_close', 'applies', 't_volume', 't_volume_price', 'fund_dwjz', 'fund_ljjz', 'fund_zzl', 'fund_hsl', 'fund_zjl']
list_node_name = ['收盘价', '涨跌幅', '成交量', '成交额', '单位净值', '累计净值', '增长率', '换手率', '折价率']

dict_node = {'t_close': ['001', '', 'DATE'], 'applies': ['002', '%', 'DATE'], 't_volume': ['003', '', 'DATE'],
             't_volume_price': ['004', '', 'DATE'], 'fund_dwjz': ['005', '', 'DATE'], 'fund_ljjz': ['006', '', 'DATE'],
             'fund_zzl': ['007', '%', 'DATE'], 'fund_hsl': ['008', '%', 'DATE'], 'fund_zjl': ['009', '%', 'DATE']}


def get_node(type_code, func_code, node):
    if node != '':
        parent_code = f"{type_code}{func_code}"
        for page in list_node:
            code = f"{parent_code}{dict_node[f'{page}'][0]}"
            if node == page:
                return code
    else:
        # 定义一个叶子空列表
        code_list = []
        # 凭借父节点
        parent_code = f"{type_code}{func_code}"
        for page in list_node:
            code = dict_node[f'{page}'][0]
            code_list.append(f"{parent_code}{code}")
        code_dict = {f"{parent_code}": code_list}
        return code_dict


def save_exp_data(code_dict, s_name, db):
    item = {}
    for k, v in code_dict.items():
        for i in range(0, len(v)):
            item['epx_parent_code'] = k
            item['epx_code'] = v[i]
            item['epx_name'] = f"{s_name}:{list_node_name[i]}"
            item['epx_units'] = dict_node[list_node[i]][1]
            item['epx_frequency'] = dict_node[list_node[i]][2]
            # 子节点
            save_exponent_sql(item, db)
            item.clear()

        # 节点
        item['epx_parent_code'] = 'ETF_start'
        item['epx_code'] = k
        item['epx_name'] = s_name
        item['epx_units'] = ''
        item['epx_frequency'] = ''
        save_exponent_sql(item, db)
        item.clear()

    # 初始节点
    item['epx_parent_code'] = 'ETF'
    item['epx_code'] = 'ETF_start'
    item['epx_name'] = 'ETF'
    item['epx_units'] = ''
    item['epx_frequency'] = ''
    save_exponent_sql(item, db)
    item.clear()

if __name__ == '__main__':
    res = get_node('ETF', '159968', '')
    save_exp_data(res, '博时中证500ETF', '232')
    save_exp_data(res, '博时中证500ETF', 'sit')
    save_exp_data(res, '博时中证500ETF', 'pro')
    print(res)