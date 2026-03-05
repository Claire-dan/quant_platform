import os
import math
import pandas as pd
import xlwings as xw
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

import get_data_order as gdo 
from ratespricer import *


def generate_start_dates(end_date, day_intervals):
    """
    根据结束日期和天数间隔列表生成开始日期列表
    
    参数:
    end_date: 结束日期(datetime/date对象)
    day_intervals: 天数间隔列表，如[0, -5, -10]表示0天前、5天前、10天前
    
    返回:
    start_dates: 开始日期列表
    """
    start_dates_list = []
    for days in day_intervals:
        start_date = d_get_bus_day(end_date, days)
        start_dates_list.append(start_date)
    return start_dates_list


def generate_pdf_report(strategy_config, ctd_config, start_dates_list, end_date, output_pdf):
    # --- 1. 分页配置 ---
    strategies_per_page = 5  # 可修改：每页显示的策略行数，可以根据清晰度自行调整(建议5-10)
    num_total_strategies = len(strategy_config)
    num_dates = len(start_dates_list)
    num_pages = math.ceil(num_total_strategies / strategies_per_page)
    
    print(f"总策略数: {num_total_strategies}, 每页显示: {strategies_per_page}, 预计总页数: {num_pages}")
    
    # 设置全局字体
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False

    # 创建 PDF 对象
    with PdfPages(output_pdf) as pdf:
        
        # --- 2. 分页循环 ---
        for page_idx in range(num_pages):
            start_row = page_idx * strategies_per_page
            end_row = min(start_row + strategies_per_page, num_total_strategies)
            current_chunk = strategy_config.iloc[start_row:end_row]
            current_num_rows = len(current_chunk)
            
            print(f"正在生成第 {page_idx + 1} 页 (策略配置第 {start_row + 1} 到 {end_row} 行)...")

            # 为当前页创建一个 Figure
            # 高度自适应：如果最后一行不满 strategies_per_page，按实际行数计算高度
            # Figure的行数：current_num_rows，列数：num_dates
            # Figure的大小：(num_dates * 10, current_num_rows * 6)
            fig, axes = plt.subplots(current_num_rows, num_dates, 
                                     figsize=(num_dates * 10, current_num_rows * 6),
                                     squeeze=False)
            
            plt.subplots_adjust(wspace=0, hspace=0.2)

            # --- 3. 策略循环（当前页内容） ---
            for s_inner_idx, (config_idx, row) in enumerate(current_chunk.iterrows()):
                strategy_name = row['strategy']
                
                for d_idx, start_date in enumerate(start_dates_list):
                    current_ax = axes[s_inner_idx, d_idx]
                    success = False
                    
                    try:
                        # 策略逻辑判断与画图调用 (传入 None 作为 row_info 避免更新 Excel)
                        if strategy_name == "基差":
                            success = gdo.get_difference_data_basis(start_date, end_date, row['code1'], row['code2'], None, ax=current_ax)
                        
                        elif strategy_name == "利差":
                            ctd_code = str(int(ctd_config.loc[ctd_config['Fut'] == row['code2'], 'Spot'].values[0]))
                            success = gdo.get_difference_data_bfspread(start_date, end_date, row['code1'], row['code2'], ctd_code, None, ax=current_ax)
                        
                        elif strategy_name == "跨期":
                            fut_code_1 = row['code1']
                            fut_code_2 = row['code2']
                            spread = row['spread']
                            ctd_code_1 = ctd_config.loc[ctd_config['Fut'] == fut_code_1, 'Spot'].values[0]
                            ctd_code_2 = ctd_config.loc[ctd_config['Fut'] == fut_code_2, 'Spot'].values[0]
                            success = gdo.get_difference_data_ffspread(start_date, end_date, fut_code_1, ctd_code_1, fut_code_2, ctd_code_2, spread, None, ax=current_ax)
                        
                        elif strategy_name == "期货利差":
                            ctd_code_1 = str(int(ctd_config.loc[ctd_config['Fut'] == row['code1'], 'Spot'].values[0]))
                            ctd_code_2 = str(int(ctd_config.loc[ctd_config['Fut'] == row['code2'], 'Spot'].values[0]))
                            success = gdo.get_difference_data_ffratespread(start_date, end_date, row['code1'], ctd_code_1, row['code2'], ctd_code_2, None, ax=current_ax)

                        elif strategy_name == "IRS利差":
                            ctd_code = str(int(ctd_config.loc[ctd_config['Fut'] == row['code2'], 'Spot'].values[0]))
                            success = gdo.get_difference_data_Ifspread(start_date, end_date, row['code1'], row['code2'], ctd_code, None, ax=current_ax)

                        elif strategy_name == "现券利差":
                            success = gdo.get_difference_data_bbspread(start_date, end_date, row['code1'], row['code2'], None, ax=current_ax)

                    except Exception as e:
                        print(f"[错误] 策略 <{strategy_name}> (配置第 {config_idx + 2} 行) 绘图失败: {e}")
                    
                    # 失败处理：在对应的子图位置留白提示
                    if not success:
                        current_ax.clear()
                        current_ax.text(0.5, 0.5, "No Data / Error", ha='center', va='center', color='gray', fontsize=20)
                        current_ax.set_xticks([])
                        current_ax.set_yticks([])

            # 将当前页保存到 PDF
            pdf.savefig(fig, bbox_inches='tight')
            
            # 及时关闭 Figure 释放内存，防止 OOM 崩溃
            plt.close(fig) 
    
    print(f"\n=============================================")
    print(f" PDF 报告生成成功: {output_pdf}")
    print(f"=============================================")


# --- 主程序入口 ---
if __name__ == "__main__":
    file_path = "report.xlsx"
    
    print("正在读取配置文件...")
    # 读取策略与最廉券配置
    strategy_config = pd.read_excel(file_path, sheet_name='初始策略配置')
    ctd_config = pd.read_excel(file_path, sheet_name='最廉券配置')
    
    # 读取日期配置
    date_config = pd.read_excel(file_path, sheet_name='日期配置')
    # raw_end_date = date_config.iloc[0, 0]  # 第一行，第一列
    # end_date = pd.to_datetime(raw_end_date) 
    end_date = datetime.now().date()
    print(f"报告日期: {end_date}")
    
    # 获取第二列的值并转换成列表
    day_intervals = date_config.iloc[:, 1].dropna().tolist()  
    start_dates_list = generate_start_dates(end_date, day_intervals)

    # 自动创建目标输出文件夹（防止因文件夹不存在而报错）
    output_dir = "./策略日报"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 统一转换时间格式生成文件名，防止非法字符报错
    if isinstance(end_date, str):
        output_filename = f"{output_dir}/策略日报_{end_date}.pdf"
    else:
        output_filename = f"{output_dir}/策略日报2606_{end_date.strftime('%Y%m%d')}.pdf"

    # 执行生成
    generate_pdf_report(strategy_config, ctd_config, start_dates_list, end_date, output_filename)



