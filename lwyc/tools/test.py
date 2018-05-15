import os
import sys
import pandas as pd
from pandas import DataFrame
import numpy as np
from sklearn import linear_model
from tools.db import *
from datetime import datetime
import re


detail = pd.read_csv('{}all/fc/{}detail.csv'.format(PATH_DATA, BIN.loc[1].ename), index_col=0)
detail = detail.set_index(['jknm', 'ceng', 'lie'])
split = detail['lw'].apply(lambda x: pd.Series(x.split("#")))
split.columns = [x + 1 for x in split.columns]
detail = detail.join(split)
detail = detail.drop('lw', axis=1)
detail.stack()
detail.unstack()
detail.reset_index(drop = True)
detail = detail.unstack()
temp1 = list(range(BIN.loc[1].left, BIN.loc[1].right + 1))  # 取有效的数据列
detail = detail[temp1]

fc_detail = pd.read_csv('{}/fc16/{}_16_detail.csv'.format(PATH_DATA, BIN.loc[1].ename), index_col=0)
fc_detail.unstack()


# 数据分仓并清洗
def fcAndClean():
    main = pd.read_csv('{}main.csv'.format(PATH_DATA), index_col=0)
    detail = pd.read_csv('{}detail.csv'.format(PATH_DATA), index_col=0)
    for meta_id in BIN.index:  # 仓号
        main_bin = main[main.cm == BIN.loc[meta_id].cname]
        main_bin.to_csv('{}all/fc/{}main.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), encoding='utf-8')
        # 根据主表来找对应的明细表
        tuo = main_bin[['jknm']]
        detail_bin = pd.merge(detail, tuo)  # , left_on='CC_LQJKQKMX_JKNM', right_on='CC_LQJKQKMX_JKNM')
        detail_bin.to_csv('{}all/fc/{}detail.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), encoding='utf-8')
        print('This is {} and we leave at {}!'.format(BIN.loc[meta_id].cname, datetime.now()))
    for meta_id in range(1, len(BIN) + 1):
        main = pd.read_csv('{}all/fc/{}main.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), index_col=0)
        detail = pd.read_csv('{}all/fc/{}detail.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), index_col=0)
        #主表清洗
        main['sj'] = main['sj'].str[7:15]  # 这个是中纺的格式，其它地方注意修改
        main['rq'] = main['rq'].map(lambda x: str(x))  # 后面这几句话的意思是把时间取成时间格式，然后设置成索引
        main['rq'] = main['rq'] + main['sj']
        main = main.drop('sj', axis=1)
        main['rq'] = main['rq'].map(lambda x: datetime.strptime(x, "%Y%m%d%H:%M:%S"))
        main = main.set_index('rq')
        main = main[main.cw < FAIR['temp_high']]
        main = main[main.cw > FAIR['temp_low']]
        main = main[main.cw != 0]
        main = main[main.ww < FAIR['temp_high']]
        main = main[main.ww > FAIR['temp_low']]
        main = main[main.ww != 0]
        #明细表清洗
        temp = main[['jknm']]
        detail = pd.merge(detail, temp)
        detail = detail.set_index(['jknm', 'ceng', 'lie'])
        split = detail['lw'].apply(lambda x: pd.Series(x.split("#")))
        split.columns = [x + 1 for x in split.columns]
        detail = detail.join(split)
        detail = detail.drop('lw', axis=1)
        detail = detail.stack()
        detail.index.names = ['jknm', 'level', 'col', 'row']
        detail = detail.unstack()
        temp1 = list(range(BIN.loc[meta_id].left, BIN.loc[meta_id].right + 1))  # 取有效的数据列
        detail = detail[temp1]
        # 去掉无效值
        detail = detail.stack()
        detail = detail.map(lambda x: float(x))
        detail[detail.values > FAIR['temp_high']] = 0
        detail[detail.values < FAIR['temp_low']] = 0
        detail[detail.values == 0] = pd.NaT
        detail = detail.unstack(['level', 'col', 'row'])
        detail = detail.stack(['level', 'col', 'row'])
        detail.name = 'lw'
        detail = DataFrame(detail)
        detail.to_csv('{}{}_fair_detail.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), encoding='utf-8')
        main.to_csv('{}{}_fair_main.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), encoding='utf-8')