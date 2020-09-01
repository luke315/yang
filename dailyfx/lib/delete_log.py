import datetime, os
import sys

# BASE_DIR
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 环境变量操作：BASE_DIR与api文件夹都要添加到环境变量
sys.path.insert(0, BASE_DIR)

path = f'{BASE_DIR}\logs'


def del_logs():
    if os.path.exists(path) and os.path.isdir(path):
        today = datetime.date.today()  # 当前日期
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        before_yesterday = datetime.date.today() - datetime.timedelta(days=2)
        file_name_list = [str(today), str(yesterday),
                          str(before_yesterday)]  # 这里面放的是要保留文件的日期,需要转成字符串格式，否则list里存的格式是<class 'datetime.date'>
        for file in os.listdir(path):  # os.listdir(path) 文件夹下所有文件名字，存在list里
            file_name_sp = file.split('.')  # 日志名字按.分割，分割成['catalina', '2017-06-13', 'log']
            if len(file_name_sp) > 2:  # 日志名字中有不带日期的，需要过滤掉,过滤掉['catalina','out']['c_']这两个文件
                file_date = file_name_sp[1]
                if file_date not in file_name_list:
                    abs_path = os.path.join(path, file)
                    print('已删除：%s' % abs_path)
                    os.remove(abs_path)
        else:
            print('没有可删除的文件')
    else:
        print('路径不存在或者不是一个目录')

def delete_logs_star():
    if os.path.exists(path):
        del_logs()
    else:
        print(path)
        print('日志地址不存在！')


if __name__ == '__main__':
    delete_logs_star()