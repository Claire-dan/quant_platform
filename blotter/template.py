from datetime import datetime
import math
import numpy as np
from pandas import DataFrame,Series
import matplotlib as mlp
import mplfinance as mplf
import pymysql
import ratespricer as rp
import pandas as pd


mlp.rcParams['axes.formatter.useoffset'] = False


def spot_data_reader(spot_code, start_date, end_date, return_type='mean', freq='1t'):
    # 获取查询结果
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond2', charset='utf8')
    cur = cnn.cursor()
    select_spot = '''
                  SELECT DateTime, Yield 
                  FROM bond_records_tick
                  WHERE BondCode = '%s' AND DateTime BETWEEN '%s 00:00:00' AND '%s 23:59:59'
                  ORDER BY DateTime
                  '''
    cur.execute(select_spot % (spot_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    rst_spot = cur.fetchall()
    cur.close()
    cnn.close()
    if len(rst_spot) < 2:
        return []
    # 将查询结果改成dataframe格式
    spot = DataFrame.from_records(list(rst_spot), columns=['datetimes', 'spot_yield']).set_index(['datetimes'])
    # 重采样
    if return_type == 'mean':
        spot = DataFrame({'spot_yield': spot['spot_yield'].resample(freq).mean(),    # 重采样取平均数
                          'spot_volume': spot['spot_yield'].resample(freq).count()}).dropna()   # 重采样取时间段内非空值的数量
        # 将date转换为"%Y-%m-%d"格式
        spot['dates'] = spot.index.map(lambda x: rp.d_to_ymd(x))
        # 计算现货净价
        spot['spot_price'] = spot.apply(lambda x: rp.b_clean_price(spot_code, x['dates'], x['spot_yield']), axis=1)
        return spot.reindex(columns=['spot_yield', 'spot_price', 'spot_volume'])
    elif return_type == 'ohlc':
        spot_ohlc = spot['spot_yield'].resample(freq).ohlc()
        spot_ohlc['volume'] = spot['spot_yield'].resample(freq).count()
        return spot_ohlc.dropna()


def futures_data_reader(fut_code, start_date, end_date, return_type='mean', freq='1t'):
    # 加载
    #cnn = pymysql.connect(host='192.168.119.53', user='yexx', passwd='yxx2023!', db='bond', charset='utf8')
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond2', charset='utf8')
    cur = cnn.cursor()
    if  return_type == 'mean':
        select_fut = '''
                     SELECT DateTime, Close, Volume 
                     FROM bond2.futures_records_1m_origin 
                     WHERE FutCode = '%s' AND DateTime BETWEEN '%s 00:00:00' AND '%s 23:59:59'
                     ORDER BY DateTime 
                     '''
        # print(f"type(start_date): {type(start_date)}")
        # print(f"type(end_date): {type(end_date)}")
        cur.execute(select_fut % (fut_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        # print(select_fut % (fut_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        rst_fut = cur.fetchall()
        cur.close()
        cnn.close()
        if len(rst_fut) < 2:
            return []
        # 处理
        fut = DataFrame.from_records(list(rst_fut), columns=['datetimes', 'fut_price', 'fut_volume']).set_index(['datetimes']).resample(freq).last()
        return fut
    elif return_type == 'ohlc':
        select_fut = '''
                     SELECT DateTime, Open, High, Low, Close, Volume 
                     FROM bond.futures_records_1m_origin 
                     WHERE FutCode = '%s' AND DateTime BETWEEN '%s 00:00:00' AND '%s 23:59:59'
                     ORDER BY DateTime 
                     '''
        cur.execute(select_fut % (fut_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        rst_fut = cur.fetchall()
        if len(rst_fut) < 2:
            return []
        # 处理
        fut = DataFrame.from_records(list(rst_fut), columns=['datetimes', 'open', 'high', 'low', 'close', 'volume']).set_index(['datetimes'])
        fut_ohlc = DataFrame({'open': fut['open'].resample(freq).first(),
                              'high': fut['high'].resample(freq).max(),
                              'low': fut['low'].resample(freq).min(),
                              'close': fut['close'].resample(freq).last(),
                              'volume': fut['volume'].resample(freq).sum()}).dropna()
        return fut_ohlc.reindex(columns=['open', 'high', 'low', 'close', 'volume'])


def rate_data_reader(rate_code, start_date, end_date):
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond2', charset='utf8')
    cur = cnn.cursor()
    select_rate = '''
                  SELECT Date, %s FROM rates WHERE Date BETWEEN '%s' AND '%s' ORDER BY Date
                  '''
    cur.execute(select_rate % (rate_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    rst_rate = cur.fetchall()
    cur.close()
    cnn.close()
    if len(rst_rate) < 2:
        return []
    # 处理
    rate = DataFrame.from_records(list(rst_rate), columns=['datetimes', rate_code]).set_index(['datetimes'])
    return rate[rate_code]


def rate_data_reader_intraday(rate_code, start_date, end_date):
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond2', charset='utf8')
    cur = cnn.cursor()
    select_rate = '''
                  SELECT Date, %s FROM rates_intraday WHERE Date BETWEEN '%s' AND '%s' ORDER BY Date
                  '''
    cur.execute(select_rate % (rate_code, start_date.strftime('%Y-%m-%d 09:30:00'), end_date.strftime('%Y-%m-%d 15:15:00')))
    rst_rate = cur.fetchall()
    cur.close()
    cnn.close()
    if len(rst_rate) < 1:
        return []
    # 处理
    rate = DataFrame.from_records(list(rst_rate), columns=['datetimes', rate_code]).set_index(['datetimes'])
    return rate[rate_code]

def irs_data_reader(irs_code, start_date, end_date, return_type='mean', freq='1min'):
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond2', charset='utf8')
    cur = cnn.cursor()
    # 读取原始tick数据(实际间隔为分钟级别）
    select_rate = '''
        SELECT Date, %s 
        FROM rates_intraday 
        WHERE Date BETWEEN '%s 00:00:00' AND '%s 23:59:59' AND %s > 0 
        ORDER BY Date
        '''
    cur.execute(select_rate % (irs_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), irs_code))
    rst_irs = cur.fetchall()
    cur.close()
    cnn.close()
    if len(rst_irs) < 2:
        return []
    # 处理
    irs = DataFrame.from_records(list(rst_irs), columns=['datetimes', 'irs_rate']).set_index(['datetimes'])

    if return_type == 'mean':
        # 对tick数据进行算数平均，转化为点数据，freq: 1min/5min/15min
        irs = DataFrame({
            'irs_rate': irs['irs_rate'].resample(freq).mean(),
            'irs_volume': irs['irs_rate'].resample(freq).count()
            }).dropna()
        return irs.reindex(columns=['irs_rate', 'irs_volume'])

    elif return_type == 'ohlc':
        # 将tick数据转化转OHLC数据，freq: freq: 15min/60min/1d
        irs_ohlc = irs['irs_rate'].resample(freq).ohlc()
        irs_ohlc['volume'] = irs['irs_rate'].resample(freq).count()
        return irs_ohlc.dropna()

def rate_data_reader_shc(rate_code, start_date, end_date):    #日度数据
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond2', charset='utf8')
    cur = cnn.cursor()
    select_rate = '''
                  SELECT Date, %s FROM rates WHERE Date BETWEEN '%s' AND '%s' ORDER BY Date
                  '''
    cur.execute(select_rate % (rate_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    rst_rate = cur.fetchall()
    cur.close()
    cnn.close()
    if len(rst_rate) < 1:
        return []
    # 处理
    rate = DataFrame.from_records(list(rst_rate), columns=['datetimes', rate_code]).set_index(['datetimes'])
    return rate[rate_code]




def repo_data_reader_intraday(repo_code, start_date, end_date):
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond2', charset='utf8')
    cur = cnn.cursor()
    select_rate = '''
                  SELECT dates, %s FROM cfets_repo_rec_1d WHERE dates BETWEEN '%s' AND '%s' ORDER BY dates
                  '''
    cur.execute(select_rate % (repo_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    rst_rate = cur.fetchall()
    cur.close()
    cnn.close()
    if len(rst_rate) < 1:
        return []
    # 处理
    rate = DataFrame.from_records(list(rst_rate), columns=['datetimes', repo_code]).set_index(['datetimes'])
    return rate[repo_code]




def spot_data_reader2(spot_code, start_date, end_date):
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond2', charset='utf8')
    cur = cnn.cursor()
    select_spot = '''
                SELECT DATE(t1.Datetime), AVG(t1.Yield) FROM bond_records_tick AS t1 
                INNER JOIN 
                (SELECT MAX(Datetime) AS Max_Datetime, BondCode FROM bond_records_tick 
                WHERE BondCode = '%s' AND DATE(Datetime) BETWEEN '%s' AND '%s' AND TIME(DateTime) BETWEEN '00:00:00' AND '16:30:00' 
                GROUP BY DATE(Datetime)) AS t2 
                ON t1.Datetime = t2.Max_Datetime AND t1.BondCode = t2.BondCode 
                GROUP BY t1.Datetime
                  '''
    cur.execute(select_spot % (spot_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    rst_spot = cur.fetchall()
    cur.close()
    cnn.close()
    if len(rst_spot) < 2:
        return []
    # 处理
    spot = DataFrame.from_records(list(rst_spot), columns=['datetimes', spot_code]).set_index(['datetimes'])
    return spot[spot_code]


def fut_data_reader2(fut_code, spot_code, start_date, end_date):
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond2', charset='utf8')
    cur = cnn.cursor()
    select_fut = '''
                 SELECT Date, FutDeliveryYield, FutCode FROM bond.futures_irr 
                 where FutMark = '%s' and BondMark = '%s' and Date BETWEEN '%s' AND '%s' ORDER BY Date
                  '''
    cur.execute(select_fut % (fut_code, spot_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    rst_fut = cur.fetchall()
    cur.close()
    cnn.close()
    if len(rst_fut) < 2:
        return []
    # 处理
    fut = DataFrame.from_records(list(rst_fut), columns=['datetimes', 'FutDeliveryYield', 'SpotCode']).set_index(['datetimes'])
    return fut

def fut_ctdrate_reader(fut_code, ctd_code, start_date, end_date):
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond2', charset='utf8')
    cur = cnn.cursor()
    select_fut = '''
                 SELECT DateTime, FutCode,BondCode,Rate FROM bond2.futures_rate_1m 
                 where FutCode = '%s' and BondCode = '%s' and DateTime BETWEEN '%s' AND '%s' ORDER BY DateTime
                  '''
    cur.execute(select_fut % (fut_code, ctd_code, start_date.strftime('%Y-%m-%d 09:30:00'), end_date.strftime('%Y-%m-%d 15:15:00')))
    rst_fut = cur.fetchall()
    cur.close()
    cnn.close()
    if len(rst_fut) < 2:
        return []
    # 处理
    fut = DataFrame.from_records(list(rst_fut), columns=['datetimes','FutCode','BondCode', 'Rate']).set_index(['datetimes'])
    return fut


def fut_ctdrate_price_reader(fut_code, ctd_code, futprice,datetime):
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond2', charset='utf8')
    cur = cnn.cursor()
    select_fut = '''
                 SELECT DateTime, FutCode,BondCode,Rate,Close FROM bond2.futures_rate_1m 
                 where FutCode = '%s' and BondCode = '%s' and DateTime = '%s' AND Close='%s' ORDER BY DateTime
                  '''
    cur.execute(select_fut % (fut_code, ctd_code, datetime.strftime('%Y-%m-%d'), futprice))
    rst_fut = cur.fetchall()
    cur.close()
    cnn.close()
    if len(rst_fut) < 2:
        return []
    # 处理
    fut = DataFrame.from_records(list(rst_fut), columns=['datetimes','FutCode','BondCode', 'Rate', 'Close']).set_index(['datetimes'])
    return fut



def basis(spot_code, fut_code, start_date, end_date, ratio, return_type='mean', freq='1t'):
    # 1、获取对冲比例
    if ratio == 'cf':
        ratio = rp.f_conversion_factor(spot_code, fut_code)
        title = ['Basis', fut_code, spot_code, 'volume']
    else:
        title = ['Spread(1:' + str(ratio) + ')', fut_code, spot_code, 'volume']  # ['Spread(1:5)', 'IF2401', 'CSI300', 'volume']
    # 2、从数据库中获取现货和期货的数据
    spot = spot_data_reader(spot_code, start_date, end_date, freq="1t")
    # print(f"spot:\n{spot}")
    if len(spot) < 2:
        return title
    fut = futures_data_reader(fut_code, start_date, end_date, freq="1t")
    # print(f"fut:\n{fut}")
    if len(fut) < 2:
        return title
    # 3、计算基差/价差
    if title[0] == 'Basis':
        df = DataFrame({title[0]: spot['spot_price'] - fut['fut_price'] * ratio,
                        title[1]: fut['fut_price'],
                        title[2]: spot['spot_yield'],
                        title[3]: spot['spot_volume']}).dropna()
    else:
        df = DataFrame({title[0]: (spot['spot_price'] - 100) - (fut['fut_price'] - 100) * ratio,
                        title[1]: fut['fut_price'],
                        title[2]: spot['spot_yield'],
                        title[3]: spot['spot_volume']}).dropna()
    df_am = df.between_time("09:30", "11:30")
    df_pm = df.between_time("13:00", "15:15")
    df = pd.concat([df_am, df_pm], axis=0)

    if return_type == 'mean':
        return df.reindex(columns=title).resample(freq).last() if len(df) >= 2 else title    # 以freq为单位进行重采样
    elif return_type == 'ohlc':
        basis_ohlc = DataFrame({'open': df[title[0]].resample(freq).first(),
                                'high': df[title[0]].resample(freq).max(),
                                'low': df[title[0]].resample(freq).min(),
                                'close': df[title[0]].resample(freq).last(),
                                'volume': df.volume.resample(freq).sum()}).dropna()
        return basis_ohlc.reindex(columns=['open', 'high', 'low', 'close', 'volume'])

  
def basis_irr(spot_code, fut_code, start_date, end_date, freq='1t'):
    title = ['irr', spot_code, fut_code]
    spot = spot_data_reader(spot_code, start_date, end_date, freq=freq)
    if len(spot) < 2:
        return 0
    fut = futures_data_reader(fut_code, start_date, end_date, freq=freq)
    if len(fut) < 2:
        return 0
    df = DataFrame({
        spot_code: spot['spot_yield'],
        fut_code: fut['fut_price'],
        'volume': spot['spot_volume']
        }).dropna().reset_index()  
    # 生成结算日
    df['dates'] = df.apply(lambda x: rp.d_to_ymd(x['datetimes']), axis=1)
    if "TS" in fut_code or "TF" in fut_code:
        df['matching_date'] = df.apply(lambda x: rp.f_final_listed_date(fut_code, -8), axis=1)
    else:
        df['matching_date'] = df.apply(lambda x: rp.f_final_listed_date(fut_code, 2), axis=1)
    df['irr'] = df.apply(lambda x: rp.f_implied_repo_rate(spot_code, x['dates'], x[spot_code], fut_code, x[fut_code], x['matching_date']), axis=1)
    return df.set_index('datetimes').reindex(columns=title) if len(df) >= 2 else 0

def get_fut_rate(fut_code, ctd_code, start_date, end_date):
    fut_code = fut_code.upper()
    fut = futures_data_reader(fut_code, start_date, end_date).resample("1t").last().ffill()
    fut_am = fut.between_time("09:30", "11:30")
    fut_pm = fut.between_time("13:00", "15:15")
    fut = pd.concat([fut_am, fut_pm], axis=0)
    fut_rate = fut["fut_price"].apply(lambda x: rp.f_implied_yield(fut_code, x, ctd_code))
    df = pd.DataFrame()
    df['close']=fut["fut_price"]
    df["fut_rate"] = fut_rate
    df["fut_code"]=str(fut_code)
    df["ctd_code"]=str(ctd_code)
    df = df.sort_index(ascending=False)
    return df

def fut_fut_irr(fut_code_1, ctd_code_1, fut_code_2, ctd_code_2, spread, start_date, end_date, freq='1t'):
    # 1. 先算近月期货隐含利率
    # 2. 加一定利差当作远月现货收益率
    # 3. 结合远月期货价格计算IRR
    title = ['irr', fut_code_1, fut_code_2]
    # 1.计算近月期货fut1的隐含收益率
    fut_rate_1 = get_fut_rate(fut_code_1, ctd_code_1, start_date, end_date)
    if len(fut_rate_1) < 2:
        return 0
    fut2 = futures_data_reader(fut_code_2, start_date, end_date, freq=freq)
    if len(fut2) < 2:
        return 0
    df = DataFrame({
        fut_code_1: fut_rate_1['fut_rate'],
        fut_code_2: fut2['fut_price'],
        'volume': fut2['fut_volume']
        }).dropna().reset_index()
    # 加一定利差当作远月现货收益率
    df[ctd_code_2] = df[fut_code_1] + spread / 100
    # 生成结算日
    df['dates'] = df.apply(lambda x: rp.d_to_ymd(x['datetimes']), axis=1)
    if "TS" in fut_code_2 or "TF" in fut_code_2:
        df['matching_date_1'] = df.apply(lambda x: rp.f_final_listed_date(fut_code_1, -8), axis=1)
        df['matching_date_2'] = df.apply(lambda x: rp.f_final_listed_date(fut_code_2, -8), axis=1)
    else:
        df['matching_date_1'] = df.apply(lambda x: rp.f_final_listed_date(fut_code_1, 2), axis=1)
        df['matching_date_2'] = df.apply(lambda x: rp.f_final_listed_date(fut_code_2, 2), axis=1)
    df['irr'] = df.apply(lambda x: rp.f_implied_repo_rate(ctd_code_2, x['matching_date_1'], x[ctd_code_2], fut_code_2, x[fut_code_2], x['matching_date_2']), axis=1)
    # print(df)
    return df.set_index('datetimes').reindex(columns=title) if len(df) >= 2 else 0

def pricespread_spot(spot_code_1, spot_code_2, start_date, end_date, return_type="mean", freq='1t'):
    title = ['Spread', spot_code_1, spot_code_2]
    spot_1 = spot_data_reader(spot_code_1, start_date, end_date, freq='1t')
    if len(spot_1) < 2:
        return title
    spot_2 = spot_data_reader(spot_code_2, start_date, end_date, freq='1t')
    if len(spot_2) < 2:
        return title

    # 利差
    if rp.b_mat_date(spot_code_2)>=rp.b_mat_date(spot_code_1):

        df = DataFrame({title[0]: (spot_2['spot_price'] - spot_1['spot_price']) * 100,
                        title[1]: spot_1['spot_price'],
                        title[2]: spot_2['spot_price']}).dropna()

    else:

        df = DataFrame({title[0]: (spot_1['spot_price'] - spot_2['spot_price']) * 100,
                        title[1]: spot_1['spot_price'],
                        title[2]: spot_2['spot_price']}).dropna()

    if return_type == 'mean':
        return df.reindex(columns=title).resample(freq).last() if len(df) >= 2 else title
    elif return_type == 'ohlc':
        spread_ohlc = DataFrame({'open': df[title[0]].resample(freq).first(),
                                'high': df[title[0]].resample(freq).max(),
                                'low': df[title[0]].resample(freq).min(),
                                'close': df[title[0]].resample(freq).last()}).dropna()
        return spread_ohlc.reindex(columns=['open', 'high', 'low', 'close'])


def spread_spot(spot_code_1, spot_code_2, start_date, end_date, return_type="mean", freq='1t'):
    title = ['spread', spot_code_1, spot_code_2]
    spot_1 = spot_data_reader(spot_code_1, start_date, end_date, freq='1t')
    if len(spot_1) < 2:
        return title
    spot_2 = spot_data_reader(spot_code_2, start_date, end_date, freq='1t')
    if len(spot_2) < 2:
        return title

    # 价差
    # if rp.b_mat_date(spot_code_2) < rp.b_mat_date(spot_code_1):

    # df = DataFrame({title[0]: (spot_2['spot_yield'] - spot_1['spot_yield']) * 100,
    #                 title[1]: spot_1['spot_yield'],
    #                 title[2]: spot_2['spot_yield']}).dropna()

    # else:

    df = DataFrame({title[0]: (spot_1['spot_yield'] - spot_2['spot_yield']) * 100,
                    title[1]: spot_1['spot_yield'],
                    title[2]: spot_2['spot_yield']}).dropna()

    if return_type == 'mean':
        return df.reindex(columns=title).resample(freq).last() if len(df) >= 2 else title
    elif return_type == 'ohlc':
        spread_ohlc = DataFrame({'open': df[title[0]].resample(freq).first(),
                                'high': df[title[0]].resample(freq).max(),
                                'low': df[title[0]].resample(freq).min(),
                                'close': df[title[0]].resample(freq).last()}).dropna()
        return spread_ohlc.reindex(columns=['open', 'high', 'low', 'close'])





def spread_futures(fut_code_1, fut_code_2, start_date, end_date, ratio=1, return_type='mean', freq='5t'):
    if ratio == 1:
        title = ['Spread', fut_code_1, fut_code_2]
    else:
        title = ['Spread(1:' + str(ratio) + ')', fut_code_1, fut_code_2]
    fut_1 = futures_data_reader(fut_code_1, start_date, end_date, freq="1t")
    if len(fut_1) < 2:
        return title
    fut_2 = futures_data_reader(fut_code_2, start_date, end_date, freq='1t')

    if len(fut_2) < 2:
        return title
    # if rp.b_fut_maturity(fut_code_1)>rp.b_fut_maturity(fut_code_2):
    #     if fut_code_1[0] == "T" and fut_code_2[0] == "T":
    #         df = DataFrame({title[0]: (fut_2['fut_price'] - 100)* ratio - (fut_1['fut_price'] - 100) ,
    #                         title[1]: fut_2['fut_price'],
    #                         title[2]: fut_1['fut_price']}).dropna()
    #     else:
    df = DataFrame({title[0]: (fut_1['fut_price'])* ratio - (fut_2['fut_price']) ,
                    title[1]: fut_1['fut_price'],
                    title[2]: fut_2['fut_price']}).dropna()
    # else:
    # if fut_code_1[0] == "T" and fut_code_2[0] == "T":
    #     df = DataFrame({title[0]: (fut_1['fut_price'] - 100)* ratio - (fut_2['fut_price'] - 100) ,
    #                     title[1]: fut_1['fut_price'],
    #                     title[2]: fut_2['fut_price']}).dropna()
    # else:
        # df = DataFrame({title[0]: (fut_1['fut_price'])* ratio - (fut_2['fut_price']) ,
        #                 title[1]: fut_1['fut_price'],
        #                 title[2]: fut_2['fut_price']}).dropna()


    if return_type == 'mean':
        return df.reindex(columns=title).resample(freq).last() if len(df) >= 2 else title
    elif return_type == 'ohlc':
        spread_ohlc = DataFrame({'open': df[title[0]].resample(freq).first(),
                                'high': df[title[0]].resample(freq).max(),
                                'low': df[title[0]].resample(freq).min(),
                                'close': df[title[0]].resample(freq).last()}).dropna()
        return spread_ohlc.reindex(columns=['open', 'high', 'low', 'close'])


def spread_rate(rate_code_1, rate_code_2, start_date, end_date):
    title = ['Spread', rate_code_1, rate_code_2]
    rate_1 = rate_data_reader(rate_code_1, start_date, end_date)
    if len(rate_1) < 2:
        return title
    rate_2 = rate_data_reader(rate_code_2, start_date, end_date)
    if len(rate_2) < 2:
        return title
    # 利差
    df = DataFrame({title[0]: (rate_2 - rate_1) * 100,
                    title[1]: rate_1,
                    title[2]: rate_2}).dropna()
    return df.reindex(columns=title) if len(df) >= 2 else title


def plot_line(df, ax, title=True):
    # 左1右1
    if not isinstance(df, DataFrame):
        ax.set_title(df[0] + ': %s * %s' % (df[1], df[2]))
        return
    # 预处理
    df = df.reset_index()
    df.rename(columns={'index': 'datetimes'}, inplace=True)
    dates = df['datetimes'].map(lambda x: rp.d_to_ymd(x))
    newdate = dates.shift(1) != dates
    ndate = len(dates[newdate])
    if ndate <= 3:
        ticks = df['datetimes'].iloc[::math.ceil(len(df) / 10)].map(lambda x: datetime.strftime(x, '%m-%d %H:%M'))
    elif 3 < ndate <= 10:
        ticks = dates[newdate].map(lambda x: datetime.strftime(x, '%m-%d'))
    elif 10 < ndate <= 250:
        ticks = dates[newdate].iloc[::math.ceil(ndate / 10)].map(lambda x: datetime.strftime(x, '%m-%d'))
    else:
        ticks = dates[newdate].iloc[::math.ceil(ndate / 10)].map(lambda x: datetime.strftime(x, '%y-%m-%d'))

    # 画图
    ax2 = ax.twinx()
    [line_1] = ax.plot(df.iloc[:, 1], 'b-', lw=1.5, label=df.iloc[:, 1].name)
    [line_2] = ax2.plot(df.iloc[:, 2], 'g-', lw=1, label=df.iloc[:, 2].name)
    ax.legend([line_1, line_2], [line_1.get_label(), line_2.get_label()+'(right)'], loc=0, fontsize=10)
    ax.set_xlim([0, len(df) - 1])
    ax.set_xticks(list(ticks.index))
    ax.set_xticklabels(list(ticks), ha='right', rotation=30, fontdict={'fontsize': 10})
    ax.grid(linestyle='--')
    if title == True:
        ax.set_title(df.columns[1] + ': %s * %s' % (df.columns[2], df.columns[3]))
    return


def plot_line2(df, ax, title=True):
    # 左2右1
    if not isinstance(df, DataFrame):
        ax.set_title(df[0] + ': %s * %s' % (df[1], df[2]))
        return
    # 预处理
    df = df.reset_index()
    df.rename(columns={'index': 'datetimes'}, inplace=True)
    dates = df['datetimes'].map(lambda x: rp.d_to_ymd(x))
    newdate = dates.shift(1) != dates
    ndate = len(dates[newdate])
    if ndate <= 3:
        ticks = df['datetimes'].iloc[::math.ceil(len(df) / 10)].map(lambda x: datetime.strftime(x, '%m-%d %H:%M'))
    elif 3 < ndate <= 10:
        ticks = dates[newdate].map(lambda x: datetime.strftime(x, '%m-%d'))
    elif 10 < ndate <= 250:
        ticks = dates[newdate].iloc[::math.ceil(ndate / 10)].map(lambda x: datetime.strftime(x, '%m-%d'))
    else:
        ticks = dates[newdate].iloc[::math.ceil(ndate / 10)].map(lambda x: datetime.strftime(x, '%y-%m-%d'))
    # 画图
    ax2 = ax.twinx()
    [line_1] = ax.plot(df.iloc[:, 1], 'b-', lw=1.5, label=df.iloc[:, 1].name)
    [line_2] = ax2.plot(df.iloc[:, 2], 'g-', lw=1, label=df.iloc[:, 2].name)
    [line_3] = ax2.plot(df.iloc[:, 3], 'y-', lw=1, label=df.iloc[:, 3].name)
    ax.legend([line_1, line_2, line_3], [line_1.get_label(), line_2.get_label()+'(right)', line_3.get_label()+'(right)'], loc=0, fontsize=8)
    ax.set_xlim([0, len(df) - 1])
    ax.set_xticks(list(ticks.index))
    ax.set_xticklabels(list(ticks), ha='right', rotation=30, fontdict={'fontsize': 8})
    ax.grid(linestyle='--')
    if title == True:
        ax.set_title(df.columns[1] + ': %s * %s' % (df.columns[2], df.columns[3]))
    return


def plot_line3(df, ax, title = True):
    # 左1
    if not isinstance(df, DataFrame):
        ax.set_title(df[0] + ': %s * %s' % (df[1], df[2]))
        return
    # 预处理
    df = df.reset_index()
    df.rename(columns={'index': 'datetimes'}, inplace=True)
    dates = df['datetimes'].map(lambda x: rp.d_to_ymd(x))
    newdate = dates.shift(1) != dates
    ndate = len(dates[newdate])
    if ndate <= 3:
        ticks = df['datetimes'].iloc[::math.ceil(len(df) / 10)].map(lambda x: datetime.strftime(x, '%m-%d %H:%M'))
    elif 3 < ndate <= 10:
        ticks = dates[newdate].map(lambda x: datetime.strftime(x, '%m-%d'))
    elif 10 < ndate <= 250:
        ticks = dates[newdate].iloc[::math.ceil(ndate / 10)].map(lambda x: datetime.strftime(x, '%m-%d'))
    else:
        ticks = dates[newdate].iloc[::math.ceil(ndate / 10)].map(lambda x: datetime.strftime(x, '%y-%m-%d'))
    # 画图
    ax2 = ax.twinx()
    [line_1] = ax.plot(df.iloc[:, 1], 'b-', lw=1.5, label=df.iloc[:, 1].name)
    # [line_2] = ax2.plot(df.iloc[:, 2], 'g-', lw=1, label=df.iloc[:, 2].name)
    # ax.legend([line_1, line_2], [line_1.get_label(), line_2.get_label()+'(right)'], loc=0, fontsize=8)
    ax.set_xlim([0, len(df) - 1])
    ax.set_xticks(list(ticks.index))
    ax.set_xticklabels(list(ticks), ha='right', rotation=30, fontdict={'fontsize': 8})
    ax.grid(linestyle='--')
    if title == True:
        ax.set_title(df.columns[1])
    return


def moving_average(x, n, type='simple'):
    """
    compute an n period moving average.
    type is 'simple' | 'exponential'
    """
    x = np.asarray(x)
    if type == 'simple':
        weights = np.ones(n)
    else:
        weights = np.exp(np.linspace(-1., 0., n))
    weights /= weights.sum()
    a = np.convolve(x, weights, mode='full')[:len(x)]
    a[:n] = np.nan
    return a


def moving_std(x, n, type='simple'):
    x = np.asarray(x)
    if type == 'simple':
        weights = np.ones(n)
    else:
        weights = np.exp(np.linspace(-1., 0., n))

    weights /= weights.sum()

    a1 = np.convolve(x, weights, mode='full')[:len(x)]
    a2 = np.convolve(x ** 2, weights, mode='full')[:len(x)]
    a = a2 - a1 ** 2
    a = np.sqrt(a)
    a[:n] = np.nan
    return a


def bollin_band(prices, n, k=2, type='simple'):
    """
    compute the n period Bollin Band
    """
    avg = moving_average(prices, n, type)
    std = moving_std(prices, n, type)

    return avg, avg + k * std, avg - k * std


def relative_strength(prices, n=14):
    """
    compute the n period relative strength indicator
    http://stockcharts.com/school/doku.php?id=chart_school:glossary_r#relativestrengthindex
    http://www.investopedia.com/terms/r/rsi.asp
    """

    deltas = np.diff(prices)
    seed = deltas[:n + 1]
    up = seed[seed >= 0].sum() / n
    down = -seed[seed < 0].sum() / n
    rs = up / down
    rsi = np.zeros_like(prices)
    rsi[:n] = 100. - 100. / (1. + rs)

    for i in range(n, len(prices)):
        delta = deltas[i - 1]  # cause the diff is 1 shorter

        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta

        up = (up * (n - 1) + upval) / n
        down = (down * (n - 1) + downval) / n

        rs = up / down
        rsi[i] = 100. - 100. / (1. + rs)

    return rsi


def moving_average_convergence(x, nslow=26, nfast=12, type='simple'):
    """
    compute the MACD (Moving Average Convergence/Divergence) using a fast and slow simple/exponential moving avg'
    return value is maslow, mafast, macd which are len(x) arrays
    """
    maslow = moving_average(x, nslow, type)
    mafast = moving_average(x, nfast, type)
    return maslow, mafast, mafast - maslow


def plot_candle(df, ax, title='', macd_params=(26, 12, 9), boll_params=(26, 2)):

    if not isinstance(df, DataFrame):
        ax.set_title(df[0] + ': %s * %s' % (df[1], df[2]))
        return

    textsize = 9

    # 预处理
    df = df.reset_index()
    df.rename(columns={'index': 'datetimes'}, inplace=True)
    dates = df['datetimes'].map(lambda x: rp.d_to_ymd(x))
    newdate = dates.shift(1) != dates
    ndate = len(dates[newdate])
    if ndate <= 3:
        ticks = df['datetimes'].iloc[::math.ceil(len(df) / 10)].map(lambda x: datetime.strftime(x, '%m-%d %H:%M'))
    elif 3 < ndate <= 10:
        ticks = dates[newdate].map(lambda x: datetime.strftime(x, '%m-%d'))
    elif 10 < ndate <= 250:
        ticks = dates[newdate].iloc[::math.ceil(ndate / 10)].map(lambda x: datetime.strftime(x, '%m-%d'))
    else:
        ticks = dates[newdate].iloc[::math.ceil(ndate / 10)].map(lambda x: datetime.strftime(x, '%y-%m-%d'))
    # 画主图
    df['time'] = Series(range(len(df)), index=df.index)
    mplf.candlestick_ohlc(ax, df[['time', 'open', 'high', 'low', 'close']].values, width=0.6, colorup='r', colordown='g')
    # 画均线
    ma5 = moving_average(df['close'], 5, type='simple')
    ma10 = moving_average(df['close'], 10, type='simple')
    ma20 = moving_average(df['close'], 20, type='simple')
    linema5, = ax.plot(df['time'], ma5, color='orange', lw=1, label='MA  5: ' + str(round(ma5[-1], 4)))
    linema10, = ax.plot(df['time'], ma10, color='red', lw=1, label='MA 10: ' + str(round(ma10[-1], 4)))
    linema20, = ax.plot(df['time'], ma20, color='purple', lw=1, label='MA 20: ' + str(round(ma20[-1], 4)))

    ax.legend([linema5, linema10, linema20],
              [linema5.get_label(), linema10.get_label(), linema20.get_label()], loc=0, fontsize=10)

    ax.set_xlim([-1, len(df)])
    ax.set_xticks(list(ticks.index))

    ax.grid(axis='both', linestyle='--')
    ax.set_xticklabels(list(ticks), ha='right', rotation=30, fontdict={'fontsize': 0})
    ax.set_xticklabels(list(ticks), ha='right', rotation=30, fontdict={'fontsize': textsize})
    if title != '':
        ax.set_title('%s' % (title))

    return


def plot_candle2(df, axes, title = '', macd_params=(26, 12, 9), boll_params=(26, 2)):
    ax1, ax2, ax3, ax4 = axes

    if not isinstance(df, DataFrame):
        ax1.set_title(df[0] + ': %s * %s' % (df[1], df[2]))
        return

    textsize = 9

    # 预处理
    df = df.reset_index()
    df.rename(columns={'index': 'datetimes'}, inplace=True)
    dates = df['datetimes'].map(lambda x: rp.d_to_ymd(x))
    newdate = dates.shift(1) != dates
    ndate = len(dates[newdate])
    if ndate <= 3:
        ticks = df['datetimes'].iloc[::math.ceil(len(df) / 10)].map(lambda x: datetime.strftime(x, '%m-%d %H:%M'))
    elif 3 < ndate <= 10:
        ticks = dates[newdate].map(lambda x: datetime.strftime(x, '%m-%d'))
    elif 10 < ndate <= 250:
        ticks = dates[newdate].iloc[::math.ceil(ndate / 10)].map(lambda x: datetime.strftime(x, '%m-%d'))
    else:
        ticks = dates[newdate].iloc[::math.ceil(ndate / 10)].map(lambda x: datetime.strftime(x, '%y-%m-%d'))
    # 画主图
    df['time'] = Series(range(len(df)), index=df.index)
    mplf.candlestick_ohlc(ax1, df[['time', 'open', 'high', 'low', 'close']].values, width=0.6, colorup='r', colordown='g')
    # 画均线
    ma5 = moving_average(df['close'], 5, type='simple')
    ma10 = moving_average(df['close'], 10, type='simple')
    ma20 = moving_average(df['close'], 20, type='simple')
    linema5, = ax1.plot(df['time'], ma5, color='orange', lw=1, label='MA  5: ' + str(round(ma5[-1], 4)))
    linema10, = ax1.plot(df['time'], ma10, color='red', lw=1, label='MA 10: ' + str(round(ma10[-1], 4)))
    linema20, = ax1.plot(df['time'], ma20, color='purple', lw=1, label='MA 20: ' + str(round(ma20[-1], 4)))

    ax1.legend([linema5, linema10, linema20],
              [linema5.get_label(), linema10.get_label(), linema20.get_label()], loc=0, fontsize=10)

    # plot the volume data

    ax2.bar(df['time'], df['volume'].values, color='blue', width = 0.6,align='center')#
    vmax = df['volume'].max()
    ax2.set_ylim(0, 1.1*vmax)

    ax2.text(0.025, 0.95, 'VOLUME', va='top', transform=ax2.transAxes, fontsize=textsize)

    #compute the MACD indicator

    fillcolor = 'darkslategrey'

    nslow, nfast, nma =  macd_params

    maslow, mafast, macd = moving_average_convergence(df['close'], nslow=nslow, nfast=nfast)
    ma9 = moving_average(macd, nma, type='simple')
    ax3.plot(df['time'], macd, color='black', lw=1)
    ax3.plot(df['time'], ma9, color='blue', lw=1)
    ax3.fill_between(df['time'], macd - ma9, 0, alpha=0.5)

    #pmax = max(macd[nslow:].max(), ma9[nslow+nma:].max(), (macd - ma9)[nslow+nma:].max())
    #pmin = min(macd[nslow:].min(), ma9[nslow+nma:].min(), (macd - ma9)[nslow+nma:].min())
    #dx = max((pmax - pmin)/8//0.01, 1)
    #ax3.set_ylim((pmin//0.01 - 0.1 * dx)/100, (pmax//0.01 + 1.1 * dx)/100)

    ax3.text(0.025, 0.95, 'MACD (%d, %d, %d)' % (nfast, nslow, nma), va='top',
             transform=ax3.transAxes, fontsize=textsize)

    #compute the BOLL indicator
    nma, nstd =  boll_params
    avgl, upl ,downl = bollin_band(df['close'], nma, nstd)
    mplf.candlestick_ohlc(ax4, df[['time', 'open', 'high', 'low', 'close']].values, width=0.6, colorup='r',
                          colordown='g')
    ax4.plot(df['time'], upl, color='orange', lw=1)
    ax4.plot(df['time'], avgl, color='blue', lw=1)
    ax4.plot(df['time'], downl, color='purple', lw=1)

    pmax = max(df['high'].max(), upl[nma:].max())
    pmin = min(df['low'].min(), downl[nma:].min())
    dx = max((pmax - pmin)/8//0.01, 1)
    ax4.set_ylim((pmin//0.01 - 0.25 * dx)/100, (pmax//0.01 + 1.25 * dx)/100)

    ax4.text(0.025, 0.95, 'BOLL (%d, %d)' % (nma, nstd), va='top',
             transform=ax4.transAxes, fontsize=textsize)

    ax1.set_xlim([-1, len(df)])
    ax1.set_xticks(list(ticks.index))

    ax1.grid(axis='both', linestyle='--')
    ax1.set_xticklabels(list(ticks), ha='right', rotation=30, fontdict={'fontsize': 0})
    ax2.grid(axis='both', linestyle='--')
    ax2.set_xticklabels(list(ticks), ha='right', rotation=30, fontdict={'fontsize': 0})
    ax3.grid(axis='both', linestyle='--')
    ax3.set_xticklabels(list(ticks), ha='right', rotation=30, fontdict={'fontsize': 0})
    ax4.grid(axis='both', linestyle='--')
    ax4.set_xticklabels(list(ticks), ha='right', rotation=30, fontdict={'fontsize': textsize})

    if title != '':
        ax1.set_title('%s' % (title))

    return
