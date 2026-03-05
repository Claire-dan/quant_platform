import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import xlwings as xw
import numpy as np
from sqlalchemy import create_engine, text
from ratespricer import *
from datetime import datetime
from blotter.template import *
import scipy.stats as stats
import matplotlib.pyplot as plt
import matplotlib.transforms as transforms

mysql_setting_local_bond2 = {
    'host': '192.168.119.53',
    'port': 3306,
    'user': 'zhongyz',
    'passwd': 'zyz2023!',
    'db': 'bond2',
    'charset': 'utf8'}
engine_local = create_engine("mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}".format(**mysql_setting_local_bond2), max_overflow=5)

mysql_setting_local_bond = {
    'host': '192.168.119.53',
    'port': 3306,
    'user': 'zhongyz',
    'passwd': 'zyz2023!',
    'db': 'bond',
    'charset': 'utf8'}
engine_local_bond2 = create_engine("mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}".format(**mysql_setting_local_bond), max_overflow=5)

mysql_setting_local_bond3 = {
    'host': '192.168.119.53',
    'port': 3306,
    'user': 'zhongyz',
    'passwd': 'zyz2023!',
    'db': 'bond3',
    'charset': 'utf8'}
engine_local_bond3 = create_engine("mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}".format(**mysql_setting_local_bond3), max_overflow=5)

path = "report.xlsx"
freq = '1T'


# ========================= 核心优化：通用工具函数 =========================

def get_t_str(start_date, end_date):
    """计算 T-N 天的统一方法，避免重复代码"""
    s_dt = d_to_ymd(start_date)
    e_dt = d_to_ymd(end_date)  
    num = d_count_bus_days(s_dt, e_dt) - 1
    return f"T-{num}"

def insert_fig_to_excel(fig, sheet_name, cell_name, pic_name):
    """统一的图表写入 Excel 逻辑，防止代码冗余并解决内存泄漏"""
    wb = xw.Book(path)
    wb.app.calculation = 'automatic'
    wb.app.visible = True
    sht = wb.sheets[sheet_name]
    sht.pictures.add(fig, left=sht.range(cell_name).left, top=sht.range(cell_name).top, name=pic_name, update=True)
    plt.close(fig) # 极其关键：插入 Excel 后立即释放该图像的内存！


# ========================= 数据获取函数 =========================

@xw.func
def get_basis(spot_code, fut_code, start_date, end_date, ratio):
    fut_code = fut_code.upper()
    df = basis(spot_code, fut_code, start_date, end_date, ratio).dropna().iloc[:, :3].sort_index(ascending=False).reset_index()
    return df.values

@xw.func
def get_spread_futures(fut_code_1, fut_code_2, start_date, end_date, ratio):
    fut_code_1 = fut_code_1.upper()
    fut_code_2 = fut_code_2.upper()
    df = spread_futures(fut_code_1, fut_code_2, start_date, end_date, ratio, freq="5t").dropna().sort_index(ascending=False).reset_index()
    return df.values

@xw.func
def get_irs_fut_spread(rate_code, fut_code, bond_code, start_date, end_date):
    fut_code = fut_code.upper()
    rate = rate_data_reader_intraday(rate_code, start_date, end_date)
    fut_rate_read = fut_ctdrate_reader(fut_code, bond_code, start_date, end_date).resample("1s").last().ffill()
    fut_rate = fut_rate_read['Rate']

    df = pd.DataFrame()
    df["spread"] = -(fut_rate - rate) * 100
    df["fut_rate"] = fut_rate
    df["rate"] = rate
    df_am = df.between_time("09:30", "11:30")
    df_pm = df.between_time("13:00", "15:15")
    df = pd.concat([df_am, df_pm], axis=0).sort_index(ascending=False).dropna().reset_index()
    return df.values

@xw.func
def get_fut_fut_spread(fut_code1, bond_code1, fut_code2, bond_code2, start_date, end_date):
    fut_code1 = fut_code1.upper()
    fut_code2 = fut_code2.upper()
    fut1 = fut_ctdrate_reader(fut_code1, bond_code1, start_date, end_date).resample("1t").last().ffill()
    fut1["datetime"] = fut1.index
    fut1 = fut1[fut1['datetime'].apply(lambda x: d_is_bus_day(x))]
    fut1_am = fut1.between_time("09:30", "11:30")
    fut1_pm = fut1.between_time("13:00", "15:15")
    fut1 = pd.concat([fut1_am, fut1_pm], axis=0)

    fut2 = fut_ctdrate_reader(fut_code2, bond_code2, start_date, end_date).resample("1t").last().ffill()
    fut2["datetime"] = fut2.index
    fut2 = fut2[fut2["datetime"].apply(lambda x: d_is_bus_day(x))]
    fut2_am = fut2.between_time("09:30", "11:30")
    fut2_pm = fut2.between_time("13:00", "15:15")
    fut2 = pd.concat([fut2_am, fut2_pm], axis=0)

    df = pd.DataFrame()
    df["spread"] = (fut1['Rate'] - fut2['Rate']) * 100
    df["fut_rate1"] = fut1['Rate']
    df["fut_rate2"] = fut2['Rate']
    df = df.sort_index(ascending=False).dropna().reset_index()
    return df.values

@xw.func
def get_spread_spot(spot_code_1, spot_code_2, start_date, end_date):
    df = spread_spot(spot_code_1, spot_code_2, start_date, end_date, freq="1min").dropna().sort_index(ascending=False).reset_index()
    return df.values

@xw.func
def get_bond_fut_spread(fut_code, ctd_code, bond_code, start_date, end_date):
    spot = spot_data_reader(bond_code, start_date, end_date, freq="1t")
    fut_code = fut_code.upper()
    fut_rate_read = fut_ctdrate_reader(fut_code, ctd_code, start_date, end_date).resample("1t").last().ffill()
    fut_rate = fut_rate_read['Rate']
    
    df = pd.DataFrame()
    df["fut_rate"] = fut_rate
    df["bond"] = spot['spot_yield']
    df["spread"] = (df['bond'] - df["fut_rate"] ) * 100

    df_am = df.between_time("09:30", "11:30")
    df_pm = df.between_time("13:00", "15:15")
    df = pd.concat([df_am, df_pm], axis=0).sort_index(ascending=False).dropna().reset_index()
    return df.values

@xw.func
def get_basis_irr(spot_code, fut_code, start_date, end_date, ratio):
    fut_code = fut_code.upper()
    re1 = basis_irr(spot_code, fut_code, start_date, end_date).dropna().iloc[:, :3].sort_index(ascending=False).reset_index()
    re2 = basis(spot_code, fut_code, start_date, end_date, ratio).dropna().iloc[:, :3].sort_index(ascending=False).reset_index()
    df = pd.merge(left=re1, right=re2) 
    df = df[['datetimes', 'irr']].sort_values(by='datetimes')
    return df.values

@xw.func
def get_fut_fut_irr(fut_code_1, ctd_code_1, fut_code_2, ctd_code_2, spread, start_date, end_date):
    fut_code_1 = fut_code_1.upper()
    fut_code_2 = fut_code_2.upper()
    df = fut_fut_irr(fut_code_1, ctd_code_1, fut_code_2, ctd_code_2, spread, start_date, end_date, freq='5t').dropna().iloc[:, :3].sort_index(ascending=False).reset_index()
    df = df[['datetimes', 'irr']].sort_values(by='datetimes')
    return df.values


# ========================= 绘图与策略包装函数 =========================

def python_spread_plot(df, underlying_name, target_str, row_info=None, ax=None):
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    
    # 修复日期格式化效率问题与秒数Bug
    if "datetime" in df.columns and pd.api.types.is_datetime64_any_dtype(df["datetime"]):
        df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M")
    
    if ax is None:
        fig, ax1 = plt.subplots(figsize=(10, 6), dpi=300)
    else:
        ax1 = ax
        fig = ax1.get_figure()
    
    ax1.plot(df["datetime"], df["difference"], linestyle='-', color='midnightblue', linewidth=5, zorder=50)

    q25 = df["difference"].quantile(0.25)
    q50 = df["difference"].quantile(0.5)
    q75 = df["difference"].quantile(0.75)
    min_val = df["difference"].min()
    max_val = df["difference"].max()
    last = df['difference'].iloc[-1]
    
    ax1.axhline(y=max_val, color='k', linestyle='--', linewidth=2)
    ax1.axhline(y=q75, color='k', linestyle='--', linewidth=2)
    ax1.axhline(y=q50, color='k', linestyle='-', linewidth=2)
    ax1.axhline(y=q25, color='k', linestyle='--', linewidth=2)
    ax1.axhline(y=min_val, color='k', linestyle='--', linewidth=2)

    trans = transforms.blended_transform_factory(ax1.transAxes, ax1.transData)
    y_range = ax1.get_ylim()[1] - ax1.get_ylim()[0]
    offset = y_range * 0.03 

    max_idx = df["difference"].values.argmax()
    min_idx = df["difference"].values.argmin()
    max_x, max_y = df["datetime"].iloc[max_idx], df["difference"].iloc[max_idx]
    min_x, min_y = df["datetime"].iloc[min_idx], df["difference"].iloc[min_idx]

    ax1.scatter([max_x], [max_y], c='r', marker='o', s=100)
    ax1.scatter([min_x], [min_y], c='r', marker='o', s=100)

    ax1.annotate(f'{max_x[5:]}', xy=(max_x, max_y), xytext=(20, 0), textcoords='offset points',
                 bbox=dict(boxstyle='round', fc='w', alpha=0.0), arrowprops=dict(arrowstyle='->'),
                 fontsize=10, color='black', weight='bold')

    ax1.annotate(f'{min_x[5:]}', xy=(min_x, min_y), xytext=(20, 0), textcoords='offset points',
                 bbox=dict(boxstyle='round', fc='w', alpha=0.0), arrowprops=dict(arrowstyle='->'),
                 fontsize=10, color='black', weight='bold')

    target_ax = ax1
    if len(df.columns) > 4:
        ax2 = ax1.twinx()
        ax2.plot(df["datetime"], df["irr"], linestyle='-', color='red', label='IRR', linewidth=1, zorder=10)
        ax2.tick_params(axis='y', labelsize=20)
        ax2.invert_yaxis()
        ax2.grid(False, axis='x')
        target_ax = ax2
        
    for val in [max_val, q75, q50, q25, min_val]:
        target_ax.text(0.90, val + offset, f'{val:.3f}', transform=trans,
                fontsize=20, fontweight='bold', va='center', ha='left', color='blue',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='black', alpha=1.0), zorder=100)

    ax1.set_title(underlying_name, fontsize=30)
    ax1.grid(True, linestyle='--', alpha=1.0, axis='y')
    ax1.tick_params(axis='x', rotation=0)

    if len(df) > 10:
        skip_value = max(1, int(len(df) / 5))
        ax1.set_xticks(df["datetime"][::skip_value])
        ax1.set_xticklabels(df["datetime"][::skip_value], fontsize=12)
    else:
        ax1.set_xticks(df["datetime"])
        ax1.set_xticklabels(df["datetime"], fontsize=12)
        
    ax1.tick_params(axis='y', labelsize=20)
    plt.tight_layout()

    if row_info is not None:
        insert_fig_to_excel(fig, '利率交易汇总', f"C{int(row_info-1)}", f"history_{target_str}")
    else:
        return fig

def python_spread_plot_modified(df, underlying_name, row):
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    
    money_df = pd.DataFrame({"date":['2025-06', '2025-07', '2025-08'], "repo":[0.016, 0.0178, 0.0173]})
    bond_code = underlying_name.split(sep="_")[0]
    df = carry_modified(df, bond_code, money_df)
    
    if pd.api.types.is_datetime64_any_dtype(df["datetime"]):
        df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M")
        
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(df["datetime"], df["difference"], linestyle='-', color='midnightblue', linewidth=5)

    q25 = df["difference"].quantile(0.25)
    q50 = df["difference"].quantile(0.5)
    q75 = df["difference"].quantile(0.75)
    min_val = df["difference"].min()
    max_val = df["difference"].max()
    
    ax1.axhline(y=max_val, color='k', linestyle='--', linewidth=2)
    ax1.axhline(y=q75, color='k', linestyle='--', linewidth=2)
    ax1.axhline(y=q50, color='k', linestyle='-', linewidth=2)
    ax1.axhline(y=q25, color='k', linestyle='--', linewidth=2)
    ax1.axhline(y=min_val, color='k', linestyle='--', linewidth=2)

    trans = transforms.blended_transform_factory(ax1.transAxes, ax1.transData)
    y_range = ax1.get_ylim()[1] - ax1.get_ylim()[0]
    offset = y_range * 0.03
    
    for val in [max_val, q75, q50, q25, min_val]:
        ax1.text(0.90, val + offset, f'{val:.2f}', transform=trans,
                 fontsize=25, fontweight='bold', va='center', ha='left', color='red',
                 bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='black', alpha=0.5))

    max_idx = df["difference"].values.argmax()
    min_idx = df["difference"].values.argmin()
    max_x, max_y = df["datetime"].iloc[max_idx], df["difference"].iloc[max_idx]
    min_x, min_y = df["datetime"].iloc[min_idx], df["difference"].iloc[min_idx]

    ax1.scatter([max_x], [max_y], c='r', marker='o', s=100)
    ax1.scatter([min_x], [min_y], c='r', marker='o', s=100)

    ax1.annotate(max_x[:10], xy=(max_x, max_y), xytext=(10, 0), textcoords='offset points',
                 bbox=dict(boxstyle='round', fc='w', alpha=0.0), arrowprops=dict(arrowstyle='->'),
                 fontsize=20, color='black', weight='bold')

    ax1.annotate(min_x[:10], xy=(min_x, min_y), xytext=(10, 0), textcoords='offset points',
                 bbox=dict(boxstyle='round', fc='w', alpha=0.0), arrowprops=dict(arrowstyle='->'),
                 fontsize=20, color='black', weight='bold')

    ax1.set_title(underlying_name, fontsize=30)
    ax1.grid(True, linestyle='--', alpha=1.0)
    ax1.tick_params(axis='x', rotation=0)
    
    if len(df) > 10:
        skip_value = max(1, int(len(df) / 5))
        ax1.set_xticks(df["datetime"][::skip_value])
        ax1.set_xticklabels(df["datetime"][::skip_value], fontsize=12)
    else:
        ax1.set_xticks(df["datetime"])
        ax1.set_xticklabels(df["datetime"], fontsize=12)
        
    ax1.tick_params(axis='y', labelsize=20)

    ax1.plot(df["datetime"], df["modified_difference"], linestyle='-', color='red', linewidth=2)
    ax2 = ax1.twinx()
    ax2.plot(df["datetime"], df["折算bp"], linestyle='--', color='black', linewidth=3)
    ax2.tick_params(axis='y', labelsize=20)

    plt.tight_layout()
    insert_fig_to_excel(fig, '利率交易汇总', 'J39', underlying_name)

def carry_modified(df, bond_code, money_df):
    df = df.copy() 
    df['year_month'] = df['datetime'].dt.to_period("M").astype(str)
    money_df['date'] = money_df['date'].astype(str)
    df = df.merge(money_df, left_on='year_month', right_on='date', how='left')
    df = df.drop(columns=['date'])
    
    last_day = df.iloc[-1]['datetime'].date()
    df['date_delta'] = (last_day - df['datetime'].dt.date).apply(lambda x: x.days)
    df['set_date'] = df['datetime'].apply(lambda x: d_get_bus_day(x, 1))
    df['dirty_price'] = df.apply(lambda x: b_dirty_price(bond_code, x['set_date'], x['bond']), axis=1)
    df['DV01'] = df.apply(lambda x: b_dollar_duration(bond_code, x['set_date'], x['bond']), axis=1)*100
    df['coup_rate'] = bond_info[bond_info['bond_code']==bond_code]['coup_rate'].values[0]/100
    df['carry'] = (df['coup_rate']*100 - df['repo']*df['dirty_price'])/100
    df['持有天数内的carry影响(元)'] = df['carry']*100*df['date_delta']/365
    df['折算bp'] = df['持有天数内的carry影响(元)']/df['DV01']*100
    df['modified_difference'] = df['difference']+df['折算bp']
    df = df[['datetime', 'difference', 'modified_difference', 'fut_rate', 'bond', '折算bp']]
    return df

today_date = d_get_bus_day(datetime.today(), 0)
if d_is_bus_day(today_date):
    today = today_date
else:
    today = d_get_bus_day(today_date, -1)

@xw.func
def get_difference_data_basis(start_date, end_date, bond_code, fut_code, row_info, ax=None):
    try:
        df = pd.DataFrame(get_basis(str(int(bond_code)), fut_code, start_date, end_date, "cf"))
        df.columns = ["datetime", "difference", "y1", "y2"]
        df_irr = pd.DataFrame(get_basis_irr(str(int(bond_code)), fut_code, start_date, end_date, "cf"))
        df_irr.columns = ["datetime", "irr"]
        df_basis_irr = pd.merge(df, df_irr, on="datetime").sort_values(by="datetime", ascending=True)
        
        target_str = get_t_str(start_date, end_date)
        python_spread_plot(df_basis_irr, f"{int(bond_code)}_{fut_code}_{target_str}", target_str, row_info, ax=ax)
        return True 
    except Exception as e:
        print(f"Basis Error ({bond_code}-{fut_code}): {e}")
        return False

@xw.func
def get_difference_data_bfspread(start_date, end_date, bond_code, fut_code, ctd_code, row, ax=None):
    try:
        df = pd.DataFrame(get_bond_fut_spread(fut_code, str(int(ctd_code)), str(int(bond_code)), start_date, end_date))
        df.columns = ["datetime", "fut_rate", "bond", "difference"]
        df = df.sort_values(by="datetime", ascending=True)
        
        target_str = get_t_str(start_date, end_date)
        python_spread_plot(df, f"{int(bond_code)}_{target_str}", target_str, row, ax=ax)
        return True
    except Exception as e:
        print(f"BF Spread Error: {e}")
        return False

@xw.func
def get_difference_data_ffspread(start_date, end_date, fut_code_1, ctd_code_1, fut_code_2, ctd_code_2, spread, row, ax=None):
    try:
        df = pd.DataFrame(get_spread_futures(fut_code_1, fut_code_2, start_date, end_date, 1))
        df.columns = ["datetime", "difference", "y1", "y2"]
        df_irr = pd.DataFrame(get_fut_fut_irr(fut_code_1, str(int(ctd_code_1)), fut_code_2, str(int(ctd_code_2)), spread, start_date, end_date))
        df_irr.columns = ["datetime", "irr"]
        df_fut_fut_irr = pd.merge(df, df_irr, on="datetime").sort_values(by="datetime", ascending=True)

        target_str = get_t_str(start_date, end_date)
        python_spread_plot(df_fut_fut_irr, f"{fut_code_1}_{fut_code_2}_{target_str}", target_str, row, ax=ax)
        return True
    except Exception as e:
        print(f"FF Spread Error: {e}")
        return False

@xw.func
def get_difference_data_Ifspread(start_date, end_date, rate_code, fut_code, ctd_code, row, ax=None):
    try:
        df = pd.DataFrame(get_irs_fut_spread(rate_code, fut_code, str(int(ctd_code)), start_date, end_date))
        df.columns = ["datetime", "difference", "y1", "y2",]
        df = df.sort_values(by="datetime", ascending=True)

        target_str = get_t_str(start_date, end_date)
        python_spread_plot(df, f"{rate_code}_{fut_code}_{target_str}", target_str, row, ax=ax)
        return True
    except Exception as e:
        print(f"IRS Spread Error: {e}")
        return False

@xw.func
def get_difference_data_bbspread(start_date, end_date, spot_code_1, spot_code_2, row, ax=None):
    try:
        df = pd.DataFrame(get_spread_spot(str(int(spot_code_1)), str(int(spot_code_2)), start_date, end_date))
        df.columns = ["datetime", "difference", "y1", "y2",]
        df = df.sort_values(by="datetime", ascending=True)
        
        target_str = get_t_str(start_date, end_date)
        python_spread_plot(df, f"{int(spot_code_1)}_{int(spot_code_2)}_{target_str}", target_str, row, ax=ax)
        return True
    except Exception as e:
        print(f"BB Spread Error: {e}")
        return False

@xw.func
def get_difference_data_ffratespread(start_date, end_date, fut_code_1, ctd_code_1, fut_code_2, ctd_code_2, row, ax=None):
    try:
        df = pd.DataFrame(get_fut_fut_spread(fut_code_1, str(int(ctd_code_1)), fut_code_2, str(int(ctd_code_2)), start_date, end_date))
        df.columns = ["datetime", "difference", "y1", "y2",]
        df = df.sort_values(by="datetime", ascending=True)
        
        target_str = get_t_str(start_date, end_date)
        python_spread_plot(df, f"{fut_code_1}_{fut_code_2}_{target_str}", target_str, row, ax=ax)
        return True
    except Exception as e:
        print(f"FF Rate Spread Error: {e}")
        return False


# ========================= 其他统计汇总图表 =========================

def python_spot_rate_plot(df, term):
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False

    fig, ax1 = plt.subplots(figsize=(12, 6))
    columns_to_plot = [col for col in df.columns if col != 'datetimes']
    default_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
    lines_info = []

    for idx, col in enumerate(columns_to_plot):
        color = default_colors[idx % len(default_colors)]
        mean_value = round(df[col].mean() * 400) / 400
        label = f"{col.split('_')[0]} (均值: {mean_value:.4f})"
        line, = ax1.plot(df['datetimes'], df[col], label=label, color=color, linewidth=2)
        lines_info.append((line, label, mean_value))

    lines_info_sorted = sorted(lines_info, key=lambda x: x[2], reverse=True)
    handles = [item[0] for item in lines_info_sorted]
    labels = [item[1] for item in lines_info_sorted]

    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    legend = ax1.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.8), fontsize=16, frameon=True, fancybox=True)
    legend.get_frame().set_edgecolor('black')
    legend.get_frame().set_linewidth(1.5)

    ax1.set_title("期货及现券收益率走势", fontsize=20)
    ax1.grid(True)
    ax1.tick_params(axis='x', rotation=0)
    
    if len(df) > 10:
        skip_value = max(1, int(len(df) / 5))
        ax1.set_xticks(df["datetimes"][::skip_value])
        ax1.set_xticklabels(df["datetimes"][::skip_value], fontsize=12)
    else:
        ax1.set_xticks(df["datetimes"])
        ax1.set_xticklabels(df["datetimes"], fontsize=12)
        
    ax1.tick_params(axis='y', labelsize=20)
    plt.tight_layout()
    insert_fig_to_excel(fig, '利率走势', 'J39', term)

@xw.func
def get_spot_rate(start_date, end_date, bond_list, fut_list):
    bond_list = [str(int(x)) for x in bond_list if x is not None]
    fut_list = [x for x in fut_list if x is not None]
    
    df_list = []
    for bond_code in bond_list:
        spot = spot_data_reader(bond_code, start_date, end_date, freq="1t")[['spot_yield']]
        spot.columns = [bond_code]
        df_list.append(spot)
        
    merged_df_value = pd.concat(df_list, axis=1)
    merged_df_value.reset_index(inplace=True)
    merged_df_value.rename(columns={'index': 'datetimes', 'datetime': 'datetimes'}, inplace=True)
    
    fut_code = fut_list[0]
    ctd_code = str(int(fut_list[1]))
    fut_rate = fut_ctdrate_reader(fut_code, ctd_code, start_date, end_date).resample("1t").last().ffill()[['Rate']]
    fut_rate.columns = [fut_code]
    fut_rate.reset_index(inplace=True)
    fut_rate.rename(columns={'index': 'datetimes', 'datetime': 'datetimes'}, inplace=True)
    
    merged_df_value = pd.merge(merged_df_value, fut_rate, on="datetimes", how="left")
    merged_df_value = merged_df_value.sort_values('datetimes', ascending=True).reset_index(drop=True).ffill().bfill()
    
    if pd.api.types.is_datetime64_any_dtype(merged_df_value['datetimes']):
        merged_df_value['datetimes'] = merged_df_value['datetimes'].dt.strftime("%Y-%m-%d %H:%M")

    python_spot_rate_plot(merged_df_value, fut_code)
    
    re_mean_rate = pd.DataFrame(merged_df_value.iloc[:, 1:].mean(), columns=['mean'])
    re_mean_rate['mean'] = round(re_mean_rate['mean'] * 400) / 400 
    mean_rate = re_mean_rate.sort_values(by='mean', ascending=False).T
    new_col = [str(col_name.split("_")[0]) for col_name in mean_rate.columns.to_list()]
    mean_rate.columns = new_col
    
    re_last = merged_df_value.iloc[-1:, 1:]
    re_last.columns = new_col
    re_last.index = ['last']
    re_last = pd.DataFrame(round(re_last.T['last'] * 400) / 400).T
    df_all = pd.concat([mean_rate, re_last], axis=0)
    
    python_rate_bar_plot(df_all, fut_code+"_rate_bar")
    
    merged_df_value.columns = ['datetimes'] + new_col
    python_rate_box_plot(merged_df_value, fut_code+"_rate_box_plot")
    return merged_df_value

@xw.func
def get_spot_rate_spread(start_date, end_date, bond_list, fut_list):
    bond_list = [str(int(x)) for x in bond_list if x is not None]
    fut_list = [x for x in fut_list if x is not None]
    fut_code = fut_list[0]
    ctd_code = str(int(fut_list[1]))
    
    df_list = []
    for bond_code in bond_list:
        df = pd.DataFrame(get_bond_fut_spread(fut_code, ctd_code, bond_code, start_date, end_date))
        df.columns = ["datetime", "fut_date", bond_code, bond_code+"_"+fut_code]
        df1 = df[['datetime', bond_code+"_"+fut_code]].set_index('datetime')
        df_list.append(df1)
        
    merged_df = pd.concat(df_list, axis=1).reset_index()
    merged_df = merged_df.sort_values('datetime', ascending=True).reset_index(drop=True).ffill().bfill()
    
    if pd.api.types.is_datetime64_any_dtype(merged_df['datetime']):
        merged_df['datetime'] = merged_df['datetime'].dt.strftime("%Y-%m-%d %H:%M")
        
    python_spot_rate_spread_plot(merged_df, fut_code+"_spread")
    
    re_mean = pd.DataFrame(merged_df.iloc[:, 1:].mean(), columns=['mean'])
    re_mean['mean'] = round(re_mean['mean'] * 20) / 20 
    mean_spread = re_mean.sort_values(by='mean', ascending=False).T
    new_col = [str(col_name.split("_")[0]) for col_name in mean_spread.columns.to_list()]
    mean_spread.columns = new_col
    
    re_last = merged_df.iloc[-1:, 1:]
    re_last.columns = new_col
    re_last.index = ['last']
    re_last = pd.DataFrame(round(re_last.T['last'] * 20) / 20).T 
    df_all = pd.concat([mean_spread, re_last], axis=0)
    
    python_spread_bar_plot(df_all, fut_code+"_spread_bar")
    
    merged_df.columns = ['datetime'] + new_col
    python_spread_box_plot(merged_df, fut_code+"_spread_box_plot")
    return merged_df

def python_spot_rate_spread_plot(df, term):
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False

    fig, ax1 = plt.subplots(figsize=(12, 6))
    columns_to_plot = [col for col in df.columns if col != 'datetime']
    default_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

    lines_info = []
    for idx, col in enumerate(columns_to_plot):
        color = default_colors[idx % len(default_colors)]
        mean_value = round(df[col].mean() * 20) / 20
        label = f"{col.split('_')[0]} (均值: {mean_value:.2f})"
        line, = ax1.plot(df['datetime'], df[col], label=label, color=color, linewidth=2)
        lines_info.append((line, label, mean_value))

    lines_info_sorted = sorted(lines_info, key=lambda x: x[2], reverse=True)
    handles = [item[0] for item in lines_info_sorted]
    labels = [item[1] for item in lines_info_sorted]

    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    legend = ax1.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.8), fontsize=16, frameon=True, fancybox=True)
    legend.get_frame().set_edgecolor('black')
    legend.get_frame().set_linewidth(1.5)

    ax1.set_title("现券期货利差走势", fontsize=20)
    ax1.grid(True)
    ax1.tick_params(axis='x', rotation=0)
    
    if len(df) > 10:
        skip_value = max(1, int(len(df) / 5))
        ax1.set_xticks(df["datetime"][::skip_value])
        ax1.set_xticklabels(df["datetime"][::skip_value], fontsize=12)
    else:
        ax1.set_xticks(df["datetime"])
        ax1.set_xticklabels(df["datetime"], fontsize=12)
        
    ax1.tick_params(axis='y', labelsize=20)
    plt.tight_layout()
    insert_fig_to_excel(fig, '利率走势', 'J39', term)

def python_spread_bar_plot(df, term):
    bond_ids = df.columns
    mean_value = list(df.values[0])
    last_value = list(df.values[1])
    x = np.arange(len(bond_ids))  
    bar_width = 0.35
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    bars1 = ax1.bar(x - bar_width/2, mean_value, width=bar_width, label='mean', color='lightcoral', edgecolor='black')
    bars2 = ax1.bar(x + bar_width/2, last_value, width=bar_width, label='last', color='lightgreen', edgecolor='black')

    ax1.set_ylim(bottom=min(df.values.flatten())-0.125, top=max(df.values.flatten())+0.125) 
    ax1.set_title('期货现货利差柱状图', fontsize=20)
    ax1.set_xticks(x)
    ax1.set_xticklabels(bond_ids, fontsize=20)
    ax1.tick_params(axis='y', labelsize=20)
    ax1.legend(fontsize=20)

    for bar in bars1 + bars2:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2, height, f'{height:.2f}', ha='center', va='bottom', fontsize=20)

    ax1.grid(True, axis='y') 
    plt.tight_layout()
    insert_fig_to_excel(fig, '利率走势', 'J39', term)

def python_rate_bar_plot(df, term):
    bond_ids = df.columns
    mean_value = list(df.values[0])
    last_value = list(df.values[1])
    x = np.arange(len(bond_ids))
    bar_width = 0.35
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    bars1 = ax1.bar(x - bar_width/2, mean_value, width=bar_width, label='mean', color='skyblue', edgecolor='black')
    bars2 = ax1.bar(x + bar_width/2, last_value, width=bar_width, label='last', color='lightgreen', edgecolor='black')

    ax1.set_ylim(bottom=min(df.values.flatten())-0.0025, top=max(df.values.flatten())+0.0025)
    ax1.set_title('债券收益率柱状图', fontsize=20)
    ax1.set_xticks(x)
    ax1.set_xticklabels(bond_ids, fontsize=20)
    ax1.tick_params(axis='y', labelsize=20)
    ax1.legend(fontsize=20)

    for bar in bars1 + bars2:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2, height, f'{height:.4f}', ha='center', va='bottom', fontsize=20)

    ax1.grid(True, axis='y') 
    plt.tight_layout()
    insert_fig_to_excel(fig, '利率走势', 'J39', term)

def python_spread_box_plot(df, term):
    bond_codes = df.columns.drop('datetime')
    mean_order = df[bond_codes].mean().sort_values(ascending=False)
    sorted_df = df[mean_order.index]
    
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.boxplot(
        sorted_df.values, patch_artist=True, labels=sorted_df.columns, widths=0.35,
        boxprops=dict(facecolor='lightblue', edgecolor='black', linewidth=2),
        whiskerprops=dict(color='black', linewidth=2),
        capprops=dict(color='black', linewidth=2),
        medianprops=dict(color='red', linewidth=2),
        showmeans=True, meanprops=dict(marker='o', markerfacecolor='white', markeredgecolor='black', markersize=6)
    )

    ax.set_title('期货现货利差分布箱型图', fontsize=20)
    ax.tick_params(axis='x', labelsize=20)
    ax.tick_params(axis='y', labelsize=20)
    ax.grid(True)

    for i, code in enumerate(sorted_df.columns):
        data = df[code].dropna()
        q1 = round(np.percentile(data, 25) * 20) / 20
        median = round(np.median(data) * 20) / 20
        q3 = round(np.percentile(data, 75) * 20) / 20 
        iqr = q3 - q1
        lower_whisker = round(data[data >= q1 - 1.5 * iqr].min() * 20) / 20
        upper_whisker = round(data[data <= q3 + 1.5 * iqr].max() * 20) / 20
        x = i + 1  
        ax.annotate(f'{lower_whisker:.2f}', xy=(x, lower_whisker), xytext=(x + 0.1, lower_whisker), fontsize=16)
        ax.annotate(f'{q1:.2f}', xy=(x, q1), xytext=(x + 0.2, q1), fontsize=16)
        ax.annotate(f'{median:.2f}', xy=(x, median), xytext=(x + 0.2, median), fontsize=16, color='red')
        ax.annotate(f'{q3:.2f}', xy=(x, q3), xytext=(x + 0.2, q3), fontsize=16)
        ax.annotate(f'{upper_whisker:.2f}', xy=(x, upper_whisker), xytext=(x + 0.1, upper_whisker), fontsize=16)
    
    insert_fig_to_excel(fig, '利率走势', 'J39', term)

def python_rate_box_plot(df, term):
    bond_codes = df.columns.drop('datetimes')
    mean_order = df[bond_codes].mean().sort_values(ascending=False)
    sorted_df = df[mean_order.index]
    
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.boxplot(
        sorted_df.values, patch_artist=True, labels=sorted_df.columns, widths=0.35,
        boxprops=dict(facecolor='lightblue', edgecolor='black', linewidth=2),
        whiskerprops=dict(color='black', linewidth=2),
        capprops=dict(color='black', linewidth=2),
        medianprops=dict(color='red', linewidth=2),
        showmeans=True, meanprops=dict(marker='o', markerfacecolor='white', markeredgecolor='black', markersize=6)
    )

    ax.set_title('债券收益率分布箱型图', fontsize=20)
    ax.tick_params(axis='x', labelsize=20)
    ax.tick_params(axis='y', labelsize=20)
    ax.grid(True)

    for i, code in enumerate(sorted_df.columns):
        data = df[code].dropna()
        q1 = round(np.percentile(data, 25) * 400) / 400
        median = round(np.median(data) * 400) / 400
        q3 = round(np.percentile(data, 75) * 400) / 400 
        iqr = q3 - q1
        lower_whisker = round(data[data >= q1 - 1.5 * iqr].min() * 400) / 400
        upper_whisker = round(data[data <= q3 + 1.5 * iqr].max() * 400) / 400
        x = i + 1  
        ax.annotate(f'{lower_whisker:.4f}', xy=(x, lower_whisker), xytext=(x + 0.1, lower_whisker), fontsize=16)
        ax.annotate(f'{q1:.4f}', xy=(x, q1), xytext=(x + 0.2, q1), fontsize=16)
        ax.annotate(f'{median:.4f}', xy=(x, median), xytext=(x + 0.2, median), fontsize=16, color='red')
        ax.annotate(f'{q3:.4f}', xy=(x, q3), xytext=(x + 0.2, q3), fontsize=16)
        ax.annotate(f'{upper_whisker:.4f}', xy=(x, upper_whisker), xytext=(x + 0.1, upper_whisker), fontsize=16)
    
    insert_fig_to_excel(fig, '利率走势', 'J39', term)


# ========================= 数据库落库与仓位获取 =========================

@xw.func
def update_position():
    today = datetime.today().date()
    pre_date = d_get_bus_day(today, -1)

    sql = f"""
        SELECT BondCode, SUM(Volume) AS Volume 
        FROM (
            SELECT BondCode, Volume 
            FROM bond.bond_positions 
            WHERE Date = '{pre_date}' AND Portfolio = 'A' 
            UNION ALL 
            SELECT BondCode, Volume 
            FROM bond.bond_blotter_trade 
            WHERE IsLast = 1 AND IsLive = 1 AND BuySell = '买入' 
            AND OrderTime < '{today}' 
            AND SettlementDate >= '{today}' AND Portfolio = 'A' 
            UNION ALL 
            SELECT BondCode, -Volume AS Volume 
            FROM bond.bond_blotter_trade 
            WHERE IsLast = 1 AND IsLive = 1 AND BuySell = '卖出' 
            AND OrderTime < '{today}' 
            AND SettlementDate >= '{today}' AND Portfolio = 'A' 
        ) AS t1 
        GROUP BY BondCode 
    """

    with engine_local_bond2.connect() as connection:
        result = connection.execute(text(sql))
        rows = result.fetchall()
        
    if not rows:
        return
    df = pd.DataFrame(rows, columns=result.keys())

    def get_maturity_group(maturity):
        if maturity <= 1: return "1Y"
        elif maturity <= 2: return "2Y"
        elif maturity <= 3: return "3Y" 
        elif maturity <= 5: return "5Y" 
        elif maturity <= 7: return "7Y" 
        elif maturity <= 10: return "10Y" 
        elif maturity <= 20: return "20Y" 
        elif maturity <= 30: return "30Y" 
        elif maturity <= 50: return "50Y" 
        else: return None

    def get_future_code(maturity):
        if maturity <= 2: return "TS2512"
        elif maturity <= 5: return "TF2512"
        elif maturity <= 10: return "T2512"
        else: return "TL2512" 

    df_position = df.groupby("BondCode")['Volume'].sum().reset_index()
    df_position.columns = ['标的1', '现有持仓']

    df_position['剩余期限']  = df_position['标的1'].apply(lambda code: b_resi_maturity(code, today))
    df_position = df_position.sort_values('剩余期限', ascending=False).reset_index(drop=True)
    df_position['期限'] = df_position['剩余期限'].apply(get_maturity_group)
    df_position['标的2'] = df_position['剩余期限'].apply(get_future_code)
    
    if len(df_position) > 20:
        print("提示：持仓债券数量超出范围，只输出前20只现券持仓。")
    df_result = df_position[['期限', '标的1', '标的2', '现有持仓']].iloc[:20]

    sht = xw.Book.caller().sheets["利率交易汇总"]
    sht.range("A2:A21").api.UnMerge()
    sht.range("A2:A21").value = None
    sht.range("B2:B21").value = None
    sht.range("E2:E21").value = None

    sht.range("A2:B21").value = df_result[['期限', '标的1']].values
    sht.range("E2:E21").value = df_result[['现有持仓']].values

    terms = df_result['期限'].unique()
    for term in terms:
        merge_start_idx = df_result.loc[df_result['期限'] == term].index.min()
        merge_end_idx = df_result.loc[df_result['期限'] == term].index.max()
        if merge_start_idx != merge_end_idx:
            delete_range = f"A{merge_start_idx+3}:A{merge_end_idx+2}"
            merge_range = f"A{merge_start_idx+2}:A{merge_end_idx+2}"
            sht.range(delete_range).value = None
            sht.range(merge_range).merge()
    
    sht.range("A2:A21").api.HorizontalAlignment = -4108
    sht.range("A2:A21").api.VerticalAlignment = -4108
    sht.range("A2:A21").api.Borders.LineStyle = 1  
    return


@xw.func
def upload_data_z2():
    sht = xw.Book.caller().sheets["利率交易汇总"]
    rng = sht.range('A1', 'K1000')
    raw_df = pd.DataFrame(rng.value).drop(columns=4).replace("", None).dropna(how='all').iloc[1:] 

    raw_df[0].ffill(inplace=True) 
    raw_df = raw_df.dropna(subset=[5,6,7,8,9,10], how='all')
    raw_df.columns = ['strategy', 'target1', 'target2', 'ratio', 'price1', 'amount1', 'price2', 'amount2', 'attention', 'trader']
    raw_df['islive'] = 'True'
    raw_df['update_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # [优化] 使用 raw connection 绕过 SQLAlchemy 对特殊符号的限制，避免替换数据失败
    update_query = "update strategy_target set islive = 'False' where islive = 'True'" 
    update_sql = '''replace into strategy_target(strategy, target1, target2, ratio, price1, amount1, price2, amount2, attention, trader, islive, datetime) 
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    raw_df = raw_df.where(raw_df.notnull(), None)  
    
    with engine_local_bond3.connect() as conn:
        conn.execute(text(update_query))
        
        # 降维回 PyMySQL 原生 cursor，执行带有 %s 的大量参数输入
        raw_conn = conn.connection
        cursor = raw_conn.cursor()
        cursor.executemany(update_sql, raw_df.values.tolist())
        raw_conn.commit()

        string = "指令已上传" + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_sql_2 = "REPLACE INTO target_response (string) VALUES (%s)"
        cursor.execute(update_sql_2, (string,))
        raw_conn.commit()
        cursor.close()

    print('updating complete【strategy_target】！' + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("--------------------------------------")
    return