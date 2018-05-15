'''
数据的预处理，包括取数据，清洗错误和空数据，求△值，得到最后拟合需要的数据
'''
import os
import sys
import pandas as pd
from pandas import DataFrame
import numpy as np
from sklearn import linear_model
from tools.db import *
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
    FROM CC_LQJKQK  '''  # +'limit 10'
    main_tuples = readdb(cmd)
    print('The length of main for all is {}!'.format(len(main_tuples)))
    main = DataFrame(list(main_tuples))
    main.columns = ['jknm', 'cm', 'rq', 'sj', 'cw', 'ww']
    main.to_csv('{}main.csv'.format(PATH_DATA), encoding='utf-8')
    return main
# 取明细表数据，层，列，粮温

def detaildata():
    cmd = '''SELECT CC_LQJKQKMX_JKNM AS jknm,
             CC_LQJKQKMX_JKCJ AS level,
             CC_LQJKQKMX_JKHS AS col,
             CC_LQJKQKMX_JCWD AS row_temp
             FROM CC_LQJKQKMX
    '''
    detail_tuple = readdb(cmd)
    print('The length of main for all is {}!'.format(len(detail_tuple)))
    detail = DataFrame(list(detail_tuple))
    detail.columns=['jknm', 'ceng', 'lie', 'lw']
    detail.to_csv('{}detail.csv'.format(PATH_DATA), encoding='utf-8')

# 数据分仓并清洗
def fcAndClean():
    maindata()
    detaildata()
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

#对每仓的数据进行处理，取出每天最接近16:00的那条数据
def fc16Data():
    for meta_id in BIN.index:
        fc_16 = DataFrame()
        fc_data = pd.read_csv('{}{}_fair_main.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), index_col='rq', parse_dates=True)
        day_range = pd.date_range(fc_data.index[0].date(), fc_data.index[-1].date(), freq='D')
        for day in day_range:
            ds_day = fc_data[day.strftime("%Y-%m-%d")]
            if len(ds_day) > 1 and (len(set(ds_day.index.hour) & {16, 15, 17}) > 0):
                for i in (16, 15, 17):
                    if( len(ds_day[ds_day.index.hour == i]) > 0 and i == 16):
                        fc_16 = fc_16.append(ds_day[ds_day.index.hour == i].iloc[0])
                        break
                    if(len(ds_day[ds_day.index.hour == i]) > 0 and i == 15):
                        fc_16 = fc_16.append(ds_day[ds_day.index.hour == i].iloc[-1])
                        break
                    if (len(ds_day[ds_day.index.hour == i]) > 0 and i == 17):
                        fc_16 = fc_16.append(ds_day[ds_day.index.hour == i].iloc[0])
                        break
        fc_16.to_csv('{}/fc16/{}_16_main.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), encoding='utf-8')
    for meta_id in BIN.index:
        fc_detail = pd.read_csv('{}{}_fair_detail.csv'.format(PATH_DATA, BIN.loc[meta_id].ename))
        fc_main = pd.read_csv('{}/fc16/{}_16_main.csv'.format(PATH_DATA, BIN.loc[meta_id].ename))
        temp = fc_main[['jknm']]
        fc_detail = pd.merge(fc_detail, temp)
        fc_detail.to_csv('{}/fc16/{}_16_detail.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), encoding='utf-8')
    for meta_id in BIN.index:
        fc_main = pd.read_csv('{}/fc16/{}_16_main.csv'.format(PATH_DATA, BIN.loc[meta_id].ename))
        fc_main.reset_index(drop = False)
        fc_main.columns = ['rq', 'cm', 'cw', 'jknm', 'ww']
        fc_detail = pd.read_csv('{}/fc16/{}_16_detail.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), index_col=0)
        fc_detail = pd.merge(fc_main, fc_detail)
        fc_detail.to_csv('{}/fc16_total/{}_16_total.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), encoding='utf-8')
        print('this is {} start'.format(BIN.loc[meta_id].ename))
#继续对detail中的数据进行处理，处理成△值
#继续对detail中的数据进行处理，处理成△值
def dataBh():
    for meta_id in BIN.index:
        fcdetail = pd.read_csv('{}/fc16_total/{}_16_total.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), index_col=0)
        test = fcdetail.copy()
        test = test[['level', 'col', 'row', 'rq', 'cw', 'ww', 'lw']]
        test = test.set_index(['level', 'col', 'row', 'rq'])
        daysData = test.unstack()
        daysData.to_csv('{}/fc16_total/{}_16_daydetail.csv'.format(PATH_DATA, BIN.loc[meta_id].ename), encoding='utf-8')




