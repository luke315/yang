# *_*coding:utf-8 *_* 
# author:  leiyang
# datetime:2020/6/4 16:20
# software: PyCharm

"""
文件说明：
"""
# excel字段栏
xlsx_args = ['股票代码', '文件名', '半导体', '芯片', 'IGBT', '晶圆', 'IDM', '设计', '代工', 'EDA', '硅片', '半导体设备', '封装测试', '电子气体', '靶材', '显影', '光刻', '涂胶', '去胶', '刻蚀', '清洗', '沉积', '离子注入', '热处理', '抛光', '测试', '硅片', '工艺化学品', '光刻胶', '电子气体', '靶材', 'CMP抛光材料', '单晶炉', '切片', '研磨', '减薄', '切割', '贴片', '键合', '封装', '分选', '基板', '引线框架', '封装树脂', '键合丝', '包装材料', '芯片粘结材料', '测试机', '分选机', '探针台', '注入用砷烷', '注入用三氟化硼', '注入用磷烷', '注入用氢气', '薄膜用硅烷', '薄膜用氨气', '刻蚀用一氧化碳', '刻蚀用二氟甲烷', '刻蚀用全氟丁二烯', '刻蚀用氟甲烷', '薄膜用丙烯', '刻蚀用甲烷', '薄膜用乙炔', '薄膜用乙硼烷', '腔体清洁用三氟化氮', '薄膜用四氟化硅', '光刻激光器用氟氩氖', '光刻激光器用氟氪氖', '刻蚀用氯气', '刻蚀用溴化氢', '刻蚀用二氧化硫', '薄膜用二氧化碳', '薄膜用一氧化二氮', '刻蚀用三氟甲烷', '刻蚀用八氟环丁烷', '刻蚀用四氟化碳', '刻蚀用六氟化硫', '光刻用氩氙氖', '合金用氢氮', '光刻用氦氮', '光刻用氪氖', '刻蚀用四氯化硅', '刻蚀用三氯化硼', '薄膜用六氟化钨', '刻蚀用八氟环戊烯', '外延用氯化氢', '外延用锗烷', '外延用乙硼烷', '外延用二氯二氢硅', '炉管用六氯乙硅烷', '炉管用氘气', '炉管用氟化氢', '薄膜用硅酸四乙酯', '薄膜用四甲基硅烷', '薄膜用八甲基环四硅氧烷', '甲基二乙氧基硅烷', '薄膜用四(二甲胺基)鉿或四氯化铪', '薄膜用四(二甲氨基)钛', '薄膜用四氯化钽或五(二甲氨基)钽', '薄膜用四氯化钛', '湿法用氢氟酸', '湿法用硫酸', '湿法用磷酸', '湿法用盐酸', '湿法用双氧水', '湿法用硝酸', '湿法用氨水', '湿法用异丙醇', '湿法用铜清洗液', '湿法用铝清洗液', '光刻用显影液', '光刻用稀释剂', 'CMP用铜抛光液', 'CMP用铜阻挡层抛光液', 'CMP用硅抛光液', 'CMP用二氧化硅抛光液', 'CMP用钨抛光液', 'CMP用铝抛光液', '铜后道用硫酸铜电镀液', '铜后道用铜电镀添加剂', '铜后道用钛靶', '铜后道用钽靶', '铜后道用铜靶', '铜后道用铝靶', 'HKMG用铝靶', 'HKMG用钛铝靶', 'Silicide用镍铂靶']
 
# 搜索指标
words = '半导体	芯片	IGBT	晶圆	IDM	设计	代工	EDA	硅片	半导体设备	封装测试	电子气体	靶材	显影	光刻	涂胶	去胶	刻蚀	清洗	沉积	离子注入	热处理	抛光	测试	硅片	工艺化学品	光刻胶	电子气体	靶材	CMP抛光材料	单晶炉	切片	研磨	减薄	切割	贴片	键合	封装	分选	基板	引线框架	封装树脂	键合丝	包装材料	芯片粘结材料	测试机	分选机	探针台	注入用砷烷	注入用三氟化硼	注入用磷烷	注入用氢气	薄膜用硅烷	薄膜用氨气	刻蚀用一氧化碳	刻蚀用二氟甲烷	刻蚀用全氟丁二烯	刻蚀用氟甲烷	薄膜用丙烯	刻蚀用甲烷	薄膜用乙炔	薄膜用乙硼烷	腔体清洁用三氟化氮	薄膜用四氟化硅	光刻激光器用氟氩氖	光刻激光器用氟氪氖	刻蚀用氯气	刻蚀用溴化氢	刻蚀用二氧化硫	薄膜用二氧化碳	薄膜用一氧化二氮	刻蚀用三氟甲烷	刻蚀用八氟环丁烷	刻蚀用四氟化碳	刻蚀用六氟化硫	光刻用氩氙氖	合金用氢氮	光刻用氦氮	光刻用氪氖	刻蚀用四氯化硅	刻蚀用三氯化硼	薄膜用六氟化钨	刻蚀用八氟环戊烯	外延用氯化氢	外延用锗烷	外延用乙硼烷	外延用二氯二氢硅	炉管用六氯乙硅烷	炉管用氘气	炉管用氟化氢	薄膜用硅酸四乙酯	薄膜用四甲基硅烷	薄膜用八甲基环四硅氧烷	甲基二乙氧基硅烷	薄膜用四(二甲胺基)鉿或四氯化铪	薄膜用四(二甲氨基)钛	薄膜用四氯化钽或五(二甲氨基)钽	薄膜用四氯化钛	湿法用氢氟酸	湿法用硫酸	湿法用磷酸	湿法用盐酸	湿法用双氧水	湿法用硝酸	湿法用氨水	湿法用异丙醇	湿法用铜清洗液	湿法用铝清洗液	光刻用显影液	光刻用稀释剂	CMP用铜抛光液	CMP用铜阻挡层抛光液	CMP用硅抛光液	CMP用二氧化硅抛光液	CMP用钨抛光液	CMP用铝抛光液	铜后道用硫酸铜电镀液	铜后道用铜电镀添加剂	铜后道用钛靶	铜后道用钽靶	铜后道用铜靶	铜后道用铝靶	HKMG用铝靶	HKMG用钛铝靶	Silicide用镍铂靶'


# 存储目录
book_name_xlsx = '../xlsx/上市公司细分指标.xls'

sheet_name_xlsx = '半导体行业'


stock_file_path = 'C:/stock_file'