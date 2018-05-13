'''
数据的预处理，包括取数据，清洗错误和空数据，求△值，得到最后拟合需要的数据
'''
import os
import sys
import pandas as pd
from pandas import DataFrame
import numpy as np
from sklearn import linear_model
from tools.db import readdb
from datetime import datetime
import re
# 取主表数据，日期时间，仓温外温
def maindata():
    cmd = '''SELECT CC_LQJKQK_NM AS jknm,
    CC_LQJKQK_CFBH AS cname,
    CC_LQJKQK_JCRQ AS date,
    CC_LQJKQK_JCSJ AS time,
    CC_LQJKQK_CFNWD AS in_bin,
    CC_LQJKQK_CFWWD AS out_bin
    FROM CC_LQJKQK WHERE CC_LQJKQK_JCSJ LIKE '16:%'  '''  # +'limit 10'
    main_tuples = readdb(cmd)
    print('The length of main for all is {}!'.format(len(main_tuples)))
    mdf = DataFrame(list(main_tuples))
    mdf.columns = ['jknm', 'cm', 'rq', 'sj', 'cw', 'ww']
    return mdf
# 取明细表数据，层，列，粮温，将粮温用#分别开，识别每一行的粮温
def detaildata():
    cmd = '''SELECT CC_LQJKQKMX_JKNM AS jknm,
             CC_LQJKQKMX_JKCJ AS level,
             CC_LQJKQKMX_JKHS AS col,
             CC_LQJKQKMX_JCWD AS row_temp
             FROM CC_LQJKQKMX
    '''
    detail_tuple = readdb(cmd)
    print('The length of main for all is {}!'.format(len(detail_tuple)))
    ddf = DataFrame(list(detail_tuple))
    ddf.columns=['jknm', 'ceng', 'lie', 'lw']
    ddf.set_index(['jknm', 'ceng', 'lie'])
    split = ddf['lw'].apply(lambda x: pd.Series(x.split("#")))
    split.columns = [x + 1 for x in split.columns]
    ddf = ddf.join(split)
    ddf = ddf.drop('lw', axis=1)
    return ddf
# 两表做连接，然后取出每天最接近16:00的那条数据
def getZhData():
    main = maindata()
    detail = detaildata()
    total = pd.merge(main, detail)
    total['rq'] = total['rq'].map(lambda x: str(x))
    total['sj'] = total['sj'].map(lambda x: str(x))
    total['sj'] = total['sj'].str[7:15]  # 这个是中纺的格式，其它地方注意修改
    total['rq'] = total['rq']+total['sj']
    total['rq'] = total['rq'].map(lambda x: datetime.strptime(x, "%Y%m%d%H:%M:%S"))
    total.drop('sj', axis=1)
    total.set_index('rq')
    day_range = pd.date_range(total.index[0].date(), total.index[-1].date(), freq='D')
    total_16 = DataFrame()
    for day in day_range:
        ds_day = total[day.strftime("%Y-%m-%d")]
        if len(ds_day) > 1 and (len(set(ds_day.index.hour) & {16, 15, 17}) > 0):
            for i in (16, 15, 17):
                if(len(ds_day[ds_day.index.hour == i]) > 0):
                    if(i == 16):
                        total_16.append(ds_day[ds_day.index.hour == i].iloc[0])
                        break
                    if(i == 15):
                        total_16.append(ds_day[ds_day.index.hour == i].iloc[-1])
                        break
                    if (i == 17):
                        total_16.append(ds_day[ds_day.index.hour == i].iloc[0])
                        break
    return total_16


