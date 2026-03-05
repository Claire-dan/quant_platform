import math
from datetime import datetime, date, timedelta
import pandas as pd
import pymysql


def d_to_ymd(date_input):
    if isinstance(date_input, date):
        return datetime(date_input.year, date_input.month, date_input.day)
    elif isinstance(date_input, str):
        return datetime.strptime(date_input, "%Y-%m-%d")


def d_is_pub_holiday(date_input):
    date_input = d_to_ymd(date_input)
    if date_input in pub_holidays:
        return True
    else:
        return False


def d_is_weekend(date_input):
    date_input = d_to_ymd(date_input)
    if date_input.weekday() in [5, 6]:  # weekday in [0,6]
        return True
    else:
        return False


def d_is_bus_day(date_input):
    date_input = d_to_ymd(date_input)
    if d_is_weekend(date_input):
        return False
    elif d_is_pub_holiday(date_input):
        return False
    else:
        return True

# 计算date_input后num_bus_days个工作日
def d_get_bus_day(date_input, num_bus_days):
    date_input = d_to_ymd(date_input)
    rolling_day, i = date_input, 0
    if num_bus_days > 0:
        while i < num_bus_days:
            rolling_day += timedelta(1)
            if d_is_bus_day(rolling_day):
                i += 1
    elif num_bus_days < 0:
        while i < -num_bus_days:
            rolling_day -= timedelta(1)
            if d_is_bus_day(rolling_day):
                i += 1
    return rolling_day


def d_count_bus_days(start_date, end_date):
    start_date = d_to_ymd(start_date)
    end_date = d_to_ymd(end_date)
    rolling_day, i = start_date, 0
    while rolling_day <= end_date:
        if d_is_bus_day(rolling_day):
            i += 1
        rolling_day += timedelta(1)
    return i


def r_value_date_by_cash(repo_code, value_date_by_posit):
    vd_posit = d_to_ymd(value_date_by_posit)
    return d_get_bus_day(vd_posit, 1)


def r_mat_date_by_posit(repo_code, value_date_by_posit):
    vd_posit = d_to_ymd(value_date_by_posit)
    md_posit = vd_posit + timedelta(int(repo_code[3:6]))
    if not d_is_bus_day(md_posit):
        md_posit = d_get_bus_day(md_posit, 1)
    return md_posit


def r_mat_date_by_cash(repo_code, value_date_by_posit):
    vd_posit = d_to_ymd(value_date_by_posit)
    md_posit = r_mat_date_by_posit(repo_code, vd_posit)
    return d_get_bus_day(md_posit, 1)


def r_count_days_by_posit(repo_code, value_date_by_posit):
    vd_posit = d_to_ymd(value_date_by_posit)
    return (r_mat_date_by_posit(repo_code, vd_posit) - vd_posit).days


def r_count_date_by_cash(repo_code, value_date_by_posit):
    vd_posit = d_to_ymd(value_date_by_posit)
    return (r_mat_date_by_cash(repo_code, vd_posit)
            - r_value_date_by_cash(repo_code, vd_posit)).days


def r_amount_by_value_date(value_date_by_posit, repo_code='all'):
    vd_posit = d_to_ymd(value_date_by_posit)
    # cnn = pymysql.connect(host='192.168.119.53', user='yexx', passwd='yxx2023!', db='bond2', charset='utf8')
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond', charset='utf8')
    cur = cnn.cursor()
    sql = "SELECT SUM(amount) / 1e8 AS amount FROM repo_rec_1d WHERE dates = '%s'" % vd_posit
    if repo_code != 'all':
        sql += " AND repo_code = '%s'" % repo_code
        cur.execute(sql)
    cur.execute(sql)
    rst = cur.fetchall()
    cur.close()
    cnn.close()
    if rst[0][0] is None:
        return 0
    return rst[0][0]


def r_value_dates_by_posit(repo_code, mat_date_by_posit):
    md_posit = d_to_ymd(mat_date_by_posit)
    value_dates = list()
    rolling_day = md_posit - timedelta(int(repo_code[3:6]))
    if not d_is_bus_day(rolling_day):
        rolling_day = d_get_bus_day(rolling_day, -1)
    while d_is_bus_day(rolling_day) and r_mat_date_by_posit(repo_code, rolling_day) == md_posit:
        value_dates.append(rolling_day)
        rolling_day -= timedelta(1)
    return value_dates


def r_amount_by_mat_date(mat_date_by_posit):
    md_posit = d_to_ymd(mat_date_by_posit)
    # cnn = pymysql.connect(host='192.168.119.53', user='yexx', passwd='yxx2023!', db='bond2', charset='utf8')
    cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond', charset='utf8')
    cur = cnn.cursor()
    sql = 'SELECT SUM(amount) / 1e8 AS amount FROM repo_rec_1d WHERE False'
    for repo_code in ['204001', '204002', '204003', '204004', '204007', '204014', '204028', '204091', '204182']:
        value_dates = r_value_dates_by_posit(repo_code, md_posit)
        for value_date in value_dates:
            sql += " OR (repo_code = '%s' AND dates = '%s')" % (repo_code, value_date.strftime('%Y-%m-%d'))
    cur.execute(sql)
    rst = cur.fetchall()
    cur.close()
    cnn.close()
    if rst[0][0] is None:
        return 0
    return rst[0][0]


def b_next_coup_date_kernel(mat_date, coup_freq, set_date):
    mat_date = d_to_ymd(mat_date)
    set_date = d_to_ymd(set_date)
    md_month = mat_date.month
    md_day = mat_date.day
    sd_year = set_date.year
    # 排除闰年影响
    if md_month == 2 and md_day == 29:
        md_day = 28
    # 利随本清
    if coup_freq == 0:
        return mat_date
    # 一年一付
    if coup_freq == 1:
        # 如果开始日期小于今年的付息日：下一付息日就是今年的付息日
        if set_date < datetime(sd_year, md_month, md_day):
            return datetime(sd_year, md_month, md_day)
        else:
            return datetime(sd_year + 1, md_month, md_day)
    # 一年两付
    if coup_freq == 2:
        the_other_cd_month = int(md_month + 6 - math.ceil(md_month / 6 - 1) * 12)  # cd := coupon date
        earlier_cd_month = min(md_month, the_other_cd_month)
        later_cd_month = max(md_month, the_other_cd_month)
        if md_month == 8 and md_day > 28:
            cd_day = 28
        elif (md_month == 1 or md_month == 7) and md_day == 31:
            cd_day = 31
        elif md_day == 31:
            cd_day = 30
        else:
            cd_day = md_day

        if set_date < datetime(sd_year, earlier_cd_month, cd_day):
            return datetime(sd_year, earlier_cd_month, cd_day)
        elif set_date < datetime(sd_year, later_cd_month, cd_day):
            return datetime(sd_year, later_cd_month, cd_day)
        else:
            return datetime(sd_year + 1, earlier_cd_month, cd_day)


def b_next_coup_date(bond_code, set_date):
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return b_next_coup_date_kernel(mat_date, coup_freq, set_date)

# 根据债券的到期日、付息频率和开始日期，计算债券的上一个付息日
# 债券的到期日主要是为了计算付息的日期（月、日），没有别的用处
# 开始日期：是计算开始日期的上一付息日
def b_pre_coup_date_kernel(mat_date, coup_freq, set_date):
    # 债券到期日
    mat_date = d_to_ymd(mat_date)
    # 开始日
    set_date = d_to_ymd(set_date)
    md_year = mat_date.year
    md_month = mat_date.month
    md_day = mat_date.day
    sd_year = set_date.year
    # 排除闰年影响
    # 如果到期日是2月29日（闰年），则将到期日调整为2月28日，避免非闰年找不到对应日期的问题
    if md_month == 2 and md_day == 29:
        md_day = 28
    # 利随本清（只在到期时一次性支付本息），则上一个付息日为到期日前一年的同一天
    if coup_freq == 0:
        return datetime(md_year - 1, md_month, md_day)
    # 一年一付
    # 付息日 = 到期日的月日（如到期日6月15日，每年都6月15日付息）
    if coup_freq == 1:
        # 如果开始日期大于等于今年付息日 → 上一个付息日 = 今年付息日
        if set_date >= datetime(sd_year, md_month, md_day):
            return datetime(sd_year, md_month, md_day)
        # 如果开始日期小于今年付息日 → 上一个付息日 = 去年付息日
        else:
            return datetime(sd_year - 1, md_month, md_day)
    # 一年两付
    if coup_freq == 2:
        # 计算另一个付息月份
        # 如果到期月份≤6，另一个月份=到期月份+6
        # 如果到期月份>6，另一个月份=到期月份-6
        the_other_cd_month = int(md_month + 6 - math.ceil(md_month / 6 - 1) * 12)  # cd := coupon date
        # 确定早晚付息月份
        # 早付息月份：较小的月份（如3月）
        # 晚付息月份：较大的月份（如9月）
        earlier_cd_month = min(md_month, the_other_cd_month)
        later_cd_month = max(md_month, the_other_cd_month)
        # 8月份：如果日期>28，一律用28日（防止2月出现29日、30日或31日）
        if md_month == 8 and md_day > 28:
            cd_day = 28
        # 1月或7月：如果原日期是31日，保持31日
        elif (md_month == 1 or md_month == 7) and md_day == 31:
            cd_day = 31
        # 其他月份：如果原日期是31日，改为30日
        elif md_day == 31:
            cd_day = 30
        # 其他情况：保持原日
        else:
            cd_day = md_day
        # 开始日期是否>=今年晚付息日：是 → 上一个付息日 = 今年晚付息日
        # 否：开始日期是否 ≥ 今年早付息日（如3月15日）？：是 → 上一个付息日 = 今年早付息日
        # 否 → 上一个付息日 = 去年晚付息日
        if set_date >= datetime(sd_year, later_cd_month, cd_day):
            return datetime(sd_year, later_cd_month, cd_day)
        elif set_date >= datetime(sd_year, earlier_cd_month, cd_day):
            return datetime(sd_year, earlier_cd_month, cd_day)
        else:
            return datetime(sd_year - 1, later_cd_month, cd_day)


def b_pre_coup_date(bond_code, set_date):
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return b_pre_coup_date_kernel(mat_date, coup_freq, set_date)


def b_resi_maturity(bond_code,set_date):
    set_date = d_to_ymd(set_date)
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    mat_date = d_to_ymd(mat_date)
    return round((mat_date-set_date).days/365,2)


def b_mat_date(bond_code):
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    mat_date = d_to_ymd(mat_date)
    return mat_date


def b_fut_maturity(fut_code):
    if "TL" in fut_code:
        return 30
    elif "TF" in fut_code:
        return 5
    elif "TS" in fut_code:
        return 2
    else:
        return 10

# 计算债券的剩余付息次数（从set_date到mat_date）
def b_count_coups_kernel(mat_date, coup_freq, set_date):
    mat_date = d_to_ymd(mat_date)
    set_date = d_to_ymd(set_date)
    # 获取  近月合约的第二交割日的 下一个付息日ncd
    next_coup_date = b_next_coup_date_kernel(mat_date, coup_freq, set_date)
    md_year = mat_date.year
    md_month = mat_date.month
    ncd_year = next_coup_date.year
    ncd_month = next_coup_date.month
    if coup_freq in [0, 1]:
        # 剩余付息次数 = 到期年份 - 下一个付息日年份 + 1
        return md_year - ncd_year + 1
    if coup_freq == 2:
        # 下一个付息日的月份=到期日的月份 → 剩余付息次数 = (到期年份 - 下一个付息日年份) * 2 + 1
        if ncd_month == md_month:
            return (md_year - ncd_year) * 2 + 1
        # 剩余付息次数 = [到期年份 - 下个付息日年份 + floor(2 - ncd_month/6)] × 2
        else:
            return int((md_year - ncd_year + math.floor(2 - ncd_month / 6)) * 2)


def b_count_coups(bond_code, set_date):
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return b_count_coups_kernel(mat_date, coup_freq, set_date)


def b_dirty_price_kernel(mat_date, coup_rate, coup_freq, set_date, ytm):
    mat_date = d_to_ymd(mat_date)
    set_date = d_to_ymd(set_date)
    # 1、获取  近月合约的第二交割日的 上一付息日pcd
    pcd = b_pre_coup_date_kernel(mat_date, coup_freq, set_date)
    # 2、获取  近月合约的第二交割日的 下一个付息日ncd
    ncd = b_next_coup_date_kernel(mat_date, coup_freq, set_date)
    c = coup_rate
    y = ytm / 100
    if coup_freq == 0:
        f = 1
    else:
        f = coup_freq   
    # 计算从 set_date（近月合约的第二交割日） 到债券到期日的剩余付息次数
    n = b_count_coups_kernel(mat_date, coup_freq, set_date)
    if n == 1:
        # 从 set_date（近月合约的第二交割日） 到下一个付息日的时间占整个付息周期的比例
        t = (ncd - set_date) / (ncd - datetime(ncd.year - 1, ncd.month, ncd.day))
        # 基于现金流折现公式：全价=现金流*折现因子
        dp = (100 + c / f) / (1 + y * t)
    else:
        t = (ncd - set_date) / (ncd - pcd)
        dp = (c / f + c / y + (100 - c / y) * (1 + y / f) ** -(n - 1)) * (1 + y / f) ** -t
    return round(dp, 8)


def b_dirty_price(bond_code, set_date, ytm):
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return b_dirty_price_kernel(mat_date, coup_rate, coup_freq, set_date, ytm)


def b_accr_int_kernel(mat_date, coup_rate, coup_freq, set_date):
    mat_date = d_to_ymd(mat_date)
    set_date = d_to_ymd(set_date)
    pcd = b_pre_coup_date_kernel(mat_date, coup_freq, set_date)
    ncd = b_next_coup_date_kernel(mat_date, coup_freq, set_date)
    c = coup_rate    
    if coup_freq == 0:
        f = 1
    else:
        f = coup_freq
    # （set_date远月合约的第二交割日 -上一个付息日）/（下一付息日-上一个付息日）
    t = (set_date - pcd) / (ncd - pcd)
    ai = c / f * t
    return round(ai, 8)


def b_accr_int(bond_code, set_date):
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return b_accr_int_kernel(mat_date, coup_rate, coup_freq, set_date)


def b_clean_price_kernel(mat_date, coup_rate, coup_freq, set_date, ytm):
    mat_date = d_to_ymd(mat_date)
    set_date = d_to_ymd(set_date)
    cp = b_dirty_price_kernel(mat_date, coup_rate, coup_freq, set_date, ytm) \
        - b_accr_int_kernel(mat_date, coup_rate, coup_freq, set_date)
    return round(cp, 4)


def b_clean_price(bond_code, set_date, ytm):
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return b_clean_price_kernel(mat_date, coup_rate, coup_freq, set_date, ytm)


def b_yield_kernel(mat_date, coup_rate, coup_freq, set_date, clean_price):
    mat_date = d_to_ymd(mat_date)
    set_date = d_to_ymd(set_date)
    y = 0
    y_left = 0.0001
    y_right = 100
    y_middle = (y_left + y_right)/2
    e = 1
    dp = clean_price + b_accr_int_kernel(mat_date, coup_rate, coup_freq, set_date)
    counter = 0
    while e > 0.00000001 and counter < 100:
        counter += 1
        y_middle = (y_left + y_right) / 2
        y = y_middle
        dp_left = b_dirty_price_kernel(mat_date, coup_rate, coup_freq, set_date, y_left)
        dp_middle = b_dirty_price_kernel(mat_date, coup_rate, coup_freq, set_date, y_middle)
        e = abs(dp_middle - dp)
        if (dp_left - dp) * (dp_middle - dp) >= 0:
            y_left = y_middle
        else:
            y_right = y_middle
    return round(y, 4)


def b_yield(bond_code, set_date, clean_price):
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return b_yield_kernel(mat_date, coup_rate, coup_freq, set_date, clean_price)


def b_dollar_duration_kernel(mat_date, coup_rate, coup_freq, set_date, ytm):
    mat_date = d_to_ymd(mat_date)
    set_date = d_to_ymd(set_date)
    dd = b_dirty_price_kernel(mat_date, coup_rate, coup_freq, set_date, ytm - 0.005) \
        - b_dirty_price_kernel(mat_date, coup_rate, coup_freq, set_date, ytm + 0.005)
    return round(dd, 4)


def b_dollar_duration(bond_code, set_date, ytm):
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return b_dollar_duration_kernel(mat_date, coup_rate, coup_freq, set_date, ytm)


def f_first_date_of_deliv_month(fut_code):
    if fut_code[0:2] in ['TL','TF', 'TS']:
        return datetime(int('20' + fut_code[2:4]), int(fut_code[4:6]), 1)   # eg：fut_code='T2603'，则返回2026年3月1日（fut_code[2:4]=26，fut_code[4:6]=03）
    else:
        return datetime(int('20' + fut_code[1:3]), int(fut_code[3:5]), 1)

# 计算期货合约最后交易日期
def f_final_listed_date(fut_code, the_nth_bus_day_following=0):
    # 获取交割月份的第一个日期，初始化周五的数量
    rolling_day, num_fridays = f_first_date_of_deliv_month(fut_code), 0
    # 循环直到找到第二个周五
    while num_fridays < 2:
        # 如果rolling_day是周五，增加周五计数器
        if rolling_day.weekday() == 4:
            num_fridays += 1
        rolling_day += timedelta(days=1)
    # 第二个周五的前一天
    fld = rolling_day - timedelta(days=1)
    # 如果是公共假日，则递延
    if d_is_pub_holiday(fld):
        fld = d_get_bus_day(fld, 1)
    return d_get_bus_day(fld, the_nth_bus_day_following)


def f_conversion_factor_kernel(mat_date, coup_rate, coup_freq, fut_code, virtual_coup_rate=3):
    mat_date = d_to_ymd(mat_date)
    fd = f_first_date_of_deliv_month(fut_code)
    ncd = b_next_coup_date_kernel(mat_date, coup_freq, fd)
    r = virtual_coup_rate / 100
    c = coup_rate / 100
    f = coup_freq
    x = (ncd.year - fd.year) * 12 + ncd.month - fd.month
    n = b_count_coups_kernel(mat_date, coup_freq, fd)

    cf = 1 / ((1 + r / f) ** (x * f / 12)) \
        * (c / f + c / r * (1 - 1 / ((1 + r / f) ** (n - 1))) + 1 / ((1 + r / f) ** (n - 1))) \
        - c / f * (12 - f * x) / 12
    return round(cf, 4)


def f_conversion_factor(bond_code, fut_code, virtual_coup_rate=3):
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return f_conversion_factor_kernel(mat_date, coup_rate, coup_freq, fut_code, virtual_coup_rate)


def f_implied_repo_rate_kernel(mat_date, coup_rate, coup_freq, set_date, ytm, fut_code, fut_price, matching_date):
    mat_date = d_to_ymd(mat_date)
    set_date = d_to_ymd(set_date)
    matching_date = d_to_ymd(matching_date)
    # 计算转换因子（远月期货合约和其对应CTD的转换因子）
    cf = f_conversion_factor_kernel(mat_date, coup_rate, coup_freq, fut_code)
    # 计算远月ctd的全价
    dp = b_dirty_price_kernel(mat_date, coup_rate, coup_freq, set_date, ytm)
    # 计算应计利息AIT（远月合约的第二交割日）
    ai = b_accr_int_kernel(mat_date, coup_rate, coup_freq, matching_date)
    # 计算 近月合约第二交割日 的下一付息日
    ncd = b_next_coup_date_kernel(mat_date, coup_freq, set_date)
    c = coup_rate
    f = coup_freq
    sum_coups_1 = 0
    sum_coups_2 = 0
    # 计算从 近月合约第二交割日 到 远月合约第二交割日 之间的所有付息金额
    while ncd < matching_date:
        # 总付息金额
        sum_coups_1 = sum_coups_1 + c / f
        # 加权付息金额
        sum_coups_2 = sum_coups_2 + c / f * (matching_date - ncd).days
        # 获取下一付息日的下一付息日
        ncd = b_next_coup_date_kernel(mat_date, coup_freq, ncd)
    irr = (fut_price * cf + ai + sum_coups_1 - dp) * 365 / (dp * (matching_date - set_date).days - sum_coups_2) * 100
    return round(irr, 4)


def f_implied_repo_rate(bond_code, set_date, ytm, fut_code, fut_price, matching_date):
    # 转换为datetime.date数据格式
    set_date = d_to_ymd(set_date)
    matching_date = d_to_ymd(matching_date)
    # 从数据库中的bond.bond_info表中获取债券的起息日、到期日、 coupon rate、 coupon freq等信息
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return f_implied_repo_rate_kernel(mat_date, coup_rate, coup_freq, set_date, ytm, fut_code, fut_price, matching_date)


def f_implied_yield(fut_code, fut_price, bond_code, matching_date=None):
    if matching_date is None:
        if "TS" in fut_code or "TF" in fut_code:
            matching_date = f_final_listed_date(fut_code, the_nth_bus_day_following=-8)
        else:
            matching_date = f_final_listed_date(fut_code, the_nth_bus_day_following=3)
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    cf = f_conversion_factor(bond_code, fut_code, virtual_coup_rate=3)
    clean_price = fut_price * cf
    return b_yield(bond_code, matching_date, clean_price)


# cnn = pymysql.connect(host='192.168.119.53', user='yexx', passwd='yxx2023!', db='bond', charset='utf8')
cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond', charset='utf8')

cur = cnn.cursor()

cur.execute("SELECT * FROM bond.pub_holidays ORDER BY dates")
rst = cur.fetchall()
pub_holidays = pd.DataFrame.from_records(list(rst), columns=['dates']).dates.map(lambda x:d_to_ymd(x)).tolist()

cur.execute("SELECT * FROM bond.bond_info ORDER BY BondCode")
rst = cur.fetchall()
bond_info = pd.DataFrame.from_records(list(rst), columns=['bond_code', 'bond_name', 'value_date', 'mat_date', 'coup_rate', 'coup_freq'])  # 起息日、到期日

cur.close()
cnn.close()

# if __name__ == "__main__":
    # iy_t = f_implied_yield("T2112",99.785, "200006")
    # iy_tf = f_implied_yield("TF2203", 102.375, "190007")
    # print(iy_tf)
    # iy_ts = f_implied_yield("TS2112", 101.34, "210002")

    # df = pd.read_excel("E:\Desktop\\4月案例.xlsx", sheet_name="期货收益率")
    # df["TF_yield"] = df["TF2106.CFE"].apply(lambda x:f_implied_yield("TF2106", x, "210002"))
    # df["T_yield"] = df["T2106.CFE"].apply(lambda x: f_implied_yield("T2106", x, "200006"))
    # df.to_excel("E:\Desktop\\期货收益率.xlsx")


