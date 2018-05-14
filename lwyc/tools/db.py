'''
通用的程序
'''
import pymysql
from pandas import DataFrame

def readdb(cmd, server= "10.24.18.101", user= "root", passwd= "lcwkd123", db= "zfdb"):
    connection = pymysql.connect(server, user, passwd, db, charset="GBK")
    cursor = connection.cursor()
    cursor.execute(cmd)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results
DEPOT='ZhongFang'
PATH_DATA = 'D:/ZhongFang/idata/corn_info/{}/'.format(DEPOT)

# 仓元数据
# 仓元数据
BIN = DataFrame({'cname': ['1号仓', '2号仓', '3号仓', '4号仓', '5号仓', '6号仓', '7号仓', '8号仓'],
                 'ename': ['Bin1', 'Bin2', 'Bin3', 'Bin4', 'Bin5', 'Bin6', 'Bin7', 'Bin8'],
                 'code': ['ff8080814ac20b70014ac23b62350006', 'ff8080814ac20b70014ac23d15230008',
                          'ff8080814ac20b70014ac23ea9e9000a', 'ff8080814ac20b70014ac23ff784000c',
                          'ff8080814ac20b70014ac2413f0c000e', 'ff8080814ac20b70014ac242834b0010',
                          'ff8080814ac20b70014ac24402b10012', 'ff8080814ac20b70014ac2454d750014'],
                 'top': [4, 4, 4, 4, 4, 4, 4, 4],
                 'bottom': [1, 1, 1, 1, 1, 1, 1, 1],
                 'left': [4, 4, 13, 13, 13, 13, 1, 1], # 一号仓从第四行开始有数据
                 'right': [23, 23, 23, 23, 23, 23, 23, 23],# 一共有23行
                 'near': [1, 1, 1, 1, 1, 1, 1, 1],
                 'far': [5, 5, 5, 5, 5, 5, 5, 5]},
                index=[1, 2, 3, 4, 5, 6, 7, 8])

# 最高最低的合理数值
FAIR = {'temp_high': 60, 'temp_low': -40}

# 有粮阶段，meta_id * 10 + 1 是起始时间，meta_id * 10 + 2 是结束时间
HAS_CORN = {11: '2015-06', 12: '2015-08', 31: '2015-12', 32: '2017-05',
            41: '2015-12', 42: '2017-05', 51: '2015-04', 52: '2017-05',
            61: '2015-04', 62: '2017-05', 71: '2015-02', 72: '2017-05'}


