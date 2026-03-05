from datetime import datetime
import pandas as pd
import numpy as np
import pymysql
import math
import warnings
from sqlalchemy import create_engine
import ratespricer as rp
import template as tp

blotter_engine = create_engine("mysql+pymysql://intern:rates2022C!@192.168.119.55:3306/Bond")

####读取基差策略及走势

def get_basis_strategy_list(date_in):
    cnn = pymysql.connect(host='192.168.119.55', user='intern', passwd='rates2022C!', db='bond', charset='utf8')
    cur = cnn.cursor()
    select_basis = '''
                 SELECT bondcode,futcode,ratio,cost FROM bond.strategy_basis where  date='%s' order by futcode,bondcode
                  '''
    cur.execute(select_basis % ( date_in.strftime('%Y-%m-%d')))
    rst_basis = cur.fetchall()
    cur.close()
    cnn.close()
    data = pd.DataFrame.from_records(list(rst_basis), columns=['bondcode', 'futcode','ratio','cost'])
    return data    


def get_basis(spot_code, fut_code, start_date, end_date, ratio):
    fut_code = fut_code.upper()
#     print(ratio)
    # if ratio==1:
    #     r="cf"
    # else:r=ratio
    df = tp.basis(spot_code, fut_code, start_date, end_date, ratio).dropna().iloc[:, :3].sort_index(
        ascending=False).reset_index()
    data = df.values
    return data

def get_basis_strategy_data( start_date, end_date):
    df_all_basis=pd.DataFrame()
    df_basis=get_basis_strategy_list(end_date)
    for spot,fut,r,cost in zip(df_basis['bondcode'],df_basis['futcode'],df_basis['ratio'],df_basis['cost']):
        if r=="cf":              
            dff=pd.DataFrame(get_basis(str(spot), str(fut), start_date, end_date,"cf" ),columns=['datetime',"basis","spot","fut"]).iloc[:,:2]
            dff['cost']=cost
            dff_out=dff.rename(columns={'basis':spot+"-"+"cf*"+fut})

        elif "TS" in fut and r=="0.5":

            dff=pd.DataFrame(get_basis(str(spot), str(fut), start_date, end_date,"cf" ),columns=['datetime',"basis","spot","fut"]).iloc[:,:2]
            dff['cost']=cost
            dff_out=dff.rename(columns={'basis':spot+"-"+"cf*"+fut})

        elif "TS" not in fut and r=="1":
            dff=pd.DataFrame(get_basis(str(spot), str(fut), start_date, end_date,"cf" ),columns=['datetime',"basis","spot","fut"]).iloc[:,:2]
            dff['cost']=cost
            dff_out=dff.rename(columns={'basis':spot+"-"+"cf*"+fut})

        else:
            dff=pd.DataFrame(get_basis(str(spot), str(fut), start_date, end_date,float(r)),columns=['datetime',"basis","spot","fut"]).iloc[:,:2]
            dff['cost']=cost
            dff_out=dff.rename(columns={'basis':spot+"-"+r+"*"+fut})

        df_all_basis=pd.concat([df_all_basis,dff_out],axis=1)
    return df_all_basis


####读取期货对冲策略及走势
def get_fut_strategy_list(date_in):
    cnn = pymysql.connect(host='192.168.119.55', user='intern', passwd='rates2022C!', db='bond', charset='utf8')
    cur = cnn.cursor()
    select = '''
                 SELECT futcode1,futcode2,ratio,cost FROM bond.strategy_fut where  date='%s' order by futcode1,futcode2
                  '''
    cur.execute(select % ( date_in.strftime('%Y-%m-%d')))
    rst = cur.fetchall()
    cur.close()
    cnn.close()
    data = pd.DataFrame.from_records(list(rst), columns=['futcode1', 'futcode2','ratio','cost'])
    return data    

def get_spread_futures(fut_code_1, fut_code_2, start_date, end_date, ratio_1, ratio_2):
    fut_code_1 = fut_code_1.upper()
    fut_code_2 = fut_code_2.upper()
    ratio = ratio_2
    df = tp.spread_futures(fut_code_1, fut_code_2, start_date, end_date, ratio, freq="1min").dropna().sort_index(
    ascending=False).reset_index()
    df.iloc[:, 1] = df.iloc[:, 1] * ratio_1
    data = df.values
    return data

def get_fut_strategy_data( start_date, end_date):
    df_all_fut=pd.DataFrame()
    df_fut=get_fut_strategy_list(start_date)
    for fut1,fut2,r,cost in zip(df_fut['futcode1'],df_fut['futcode2'],df_fut['ratio'],df_fut['cost']):

        dff_fut=pd.DataFrame(get_spread_futures(fut1, fut2, start_date, end_date,1,float(r)),columns=['datetime',"basis","fut1","fut2"]).iloc[:,:2]
        dff_fut['cost']=cost
        dff_out_fut=dff_fut.rename(columns={'basis':str(fut2)+"-"+str(r)+"*"+str(fut1)})


        df_all_fut=pd.concat([df_all_fut,dff_out_fut],axis=1)
    return df_all_fut


####读取期货-IRS策略及走势
def get_fut_irs_strategy_list(date_in):
    cnn = pymysql.connect(host='192.168.119.55', user='intern', passwd='rates2022C!', db='bond', charset='utf8')
    cur = cnn.cursor()
    select = '''
                 SELECT futcode,irs,futctd,ratio,cost FROM bond.strategy_fut_irs where  date='%s' order by futcode
                  '''
    cur.execute(select % ( date_in.strftime('%Y-%m-%d')))
    rst = cur.fetchall()
    cur.close()
    cnn.close()
    data = pd.DataFrame.from_records(list(rst), columns=['futcode', 'irs','futctd','ratio','cost'])
    return data    

def get_irs_fut_spread(rate_code, fut_code, bond_code, start_date, end_date):
    fut_code = fut_code.upper()
    rate = tp.rate_data_reader_intraday(rate_code, start_date, end_date)
    fut_rate_read = tp.fut_ctdrate_reader(fut_code, bond_code, start_date, end_date).resample("60s").last().ffill()
    fut_rate=fut_rate_read['Rate']
    df = pd.DataFrame()
    if "TF" in fut_code or "TS" in fut_code:
        df["spread"] = (rate-fut_rate ) * 100
    else:
        df["spread"] = (fut_rate - rate) * 100
    df["fut_rate"] = fut_rate
    df["rate"] = rate

    df_am = df.between_time("09:30", "11:30")
    df_pm = df.between_time("13:30", "15:15")
    df = pd.concat([df_am, df_pm], axis=0)
    df = df.sort_index(ascending=False)

    df = df.dropna().reset_index()
    data = df.values
    return data


def get_fut_irs_strategy_data( start_date, end_date):
    df_all_fut_irs=pd.DataFrame()
    df_fut_irs= get_fut_irs_strategy_list(end_date)
    for fut,irs,ctd,r,cost in zip(df_fut_irs['futcode'],df_fut_irs['irs'],df_fut_irs['futctd'],df_fut_irs['ratio'],df_fut_irs['cost']):

        dff_fut_irs=pd.DataFrame(get_irs_fut_spread(irs, fut, ctd,start_date, end_date),columns=['datetime',"basis","fut1","fut2"]).iloc[:,:2]
        dff_fut_irs['cost']=cost
        dff_out_fut_irs=dff_fut_irs.rename(columns={'basis':str(irs)+"-"+str(r)+"*"+str(fut)+"("+str(ctd)+")"})


        df_all_fut_irs=pd.concat([df_all_fut_irs,dff_out_fut_irs],axis=1)
    return df_all_fut_irs


####读取现券-IRS策略及走势

def get_bond_irs_strategy_list(date_in):
    cnn = pymysql.connect(host='192.168.119.55', user='intern', passwd='rates2022C!', db='bond', charset='utf8')
    cur = cnn.cursor()
    select = '''
                 SELECT irs,bondcode,ratio,cost FROM bond.strategy_bond_irs where  date='%s' order by bondcode
                  '''
    cur.execute(select % ( date_in.strftime('%Y-%m-%d')))
    rst = cur.fetchall()
    cur.close()
    cnn.close()
    data = pd.DataFrame.from_records(list(rst), columns=['irs', 'bondcode','ratio','cost'])
    return data    

def get_irs_bond_spread(rate_code, bond_code, start_date, end_date):
    rate = tp.rate_data_reader_intraday(rate_code, start_date, end_date).resample("1s").ffill()
    spot = tp.spot_data_reader(bond_code, start_date, end_date, freq="1t")
    rate = rate.reindex(spot.index).ffill()
    df = pd.DataFrame()
    resi_maturity=rp.b_resi_maturity(bond_code, end_date)  #剩余期限
    if resi_maturity>=5:
        df["spread"] = (spot["spot_yield"] - rate) * 100
    else:
        df["spread"] = -(spot["spot_yield"] - rate) * 100
    df["spot"] = spot["spot_yield"]
    df["rate"] = rate
    df_am = df.between_time("09:00", "12:00")
    df_pm = df.between_time("13:30", "17:30")
    df = pd.concat([df_am, df_pm], axis=0)
    df = df.sort_index(ascending=False)
    df = df.dropna().sort_index(ascending=False).reset_index()
    data = df.values
    return data

def get_bond_irs_strategy_data( start_date, end_date):
    df_all_bond_irs=pd.DataFrame()
    df_bond_irs=get_bond_irs_strategy_list(end_date)
    for irs,bondcode,r,cost in zip(df_bond_irs['irs'],df_bond_irs['bondcode'],df_bond_irs['ratio'],df_bond_irs['cost']):

        dff_bond_irs=pd.DataFrame(get_irs_bond_spread(irs, bondcode,start_date, end_date),columns=['datetime',"basis","irs","bondcode"]).iloc[:,:2]
        dff_bond_irs['cost']=cost
        dff_out_bond_irs=dff_bond_irs.rename(columns={'basis':str(r)+"*"+str(bondcode)+"-"+str(irs)})


        df_all_bond_irs=pd.concat([df_all_bond_irs,dff_out_bond_irs],axis=1)
    return df_all_bond_irs


####读取现券利差策略及走势
def get_bondspread_strategy_list(date_in):
    cnn = pymysql.connect(host='192.168.119.55', user='intern', passwd='rates2022C!', db='bond', charset='utf8')
    cur = cnn.cursor()
    select = '''
                 SELECT bondcode1,bondcode2,ratio,cost FROM bond.strategy_bondspread where  date='%s' order by bondcode1
                  '''
    cur.execute(select % ( date_in.strftime('%Y-%m-%d')))
    rst = cur.fetchall()
    cur.close()
    cnn.close()
    data = pd.DataFrame.from_records(list(rst), columns=['bondcode1', 'bondcode2','ratio','cost'])
    return data    

def get_spread_spot(spot_code_1, spot_code_2, start_date, end_date):
    df = tp.spread_spot(spot_code_1, spot_code_2, start_date, end_date, freq="1min").dropna().sort_index(
        ascending=False).reset_index()
    print(df)
    data = df.values
    return data

def get_bondspread_strategy_data( start_date, end_date):
    df_all_bondspread=pd.DataFrame()
    df_bondspread=get_bondspread_strategy_list(end_date)
    for bondcode1,bondcode2,r,cost in zip(df_bondspread['bondcode1'],df_bondspread['bondcode2'],df_bondspread['ratio'],df_bondspread['cost']):

        dff_bond=pd.DataFrame(get_spread_spot(bondcode1, bondcode2,start_date, end_date),columns=['datetime',"basis","bondcode1","bondcode2"]).iloc[:,:2]
        dff_bond['cost']=cost
        dff_out_bond=dff_bond.rename(columns={'basis':str(bondcode1)+"-"+str(bondcode2)})


        df_all_bondspread=pd.concat([df_all_bondspread,dff_out_bond],axis=1)
    return df_all_bondspread

def get_allstrategy_data( start_date, end_date):
    df_basis=get_basis_strategy_data( start_date, end_date)
    df_fut=get_fut_strategy_data( start_date, end_date)
    df_fut_irs=get_fut_irs_strategy_data( start_date, end_date)
    df_bond_irs=get_bond_irs_strategy_data( start_date, end_date)
    df_bondspread=get_bondspread_strategy_data( start_date, end_date)

    df_out=pd.concat([df_basis,df_fut,df_fut_irs,df_bond_irs,df_bondspread],axis=1)

    return df_out


end_date=rp.d_get_bus_day(datetime.today(), -1)
start_date=rp.d_get_bus_day(end_date, -10)
# print(get_basis_strategy_list(end_date))
print(get_allstrategy_data( start_date, end_date))
