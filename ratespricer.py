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

# 根据输入日期计算前/后若干个工作日的日期
def d_get_bus_day(date_input, num_bus_days):
    date_input = d_to_ymd(date_input)   # 将日期转换为datetime.date格式
    rolling_day, i = date_input, 0
    if num_bus_days > 0:
        # 向后计算工作日
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
    cnn = pymysql.connect(host='192.168.119.53', user='yexx', passwd='yxx2023!', db='bond2', charset='utf8')
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
    cnn = pymysql.connect(host='192.168.119.53', user='yexx', passwd='yxx2023!', db='bond2', charset='utf8')
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


def b_pre_coup_date_kernel(mat_date, coup_freq, set_date):
    mat_date = d_to_ymd(mat_date)
    set_date = d_to_ymd(set_date)
    md_year = mat_date.year
    md_month = mat_date.month
    md_day = mat_date.day
    sd_year = set_date.year
    # 排除闰年影响
    if md_month == 2 and md_day == 29:
        md_day = 28
    # 利随本清
    if coup_freq == 0:
        return datetime(md_year - 1, md_month, md_day)
    # 一年一付
    if coup_freq == 1:
        if set_date >= datetime(sd_year, md_month, md_day):
            return datetime(sd_year, md_month, md_day)
        else:
            return datetime(sd_year - 1, md_month, md_day)
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

def b_count_coups_kernel(mat_date, coup_freq, set_date):
    mat_date = d_to_ymd(mat_date)
    set_date = d_to_ymd(set_date)
    next_coup_date = b_next_coup_date_kernel(mat_date, coup_freq, set_date)
    md_year = mat_date.year
    md_month = mat_date.month
    ncd_year = next_coup_date.year
    ncd_month = next_coup_date.month
    if coup_freq in [0, 1]:
        return md_year - ncd_year + 1
    if coup_freq == 2:
        if ncd_month == md_month:
            return (md_year - ncd_year) * 2 + 1
        else:
            return int((md_year - ncd_year + math.floor(2 - ncd_month / 6)) * 2)


def b_count_coups(bond_code, set_date):
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return b_count_coups_kernel(mat_date, coup_freq, set_date)


def b_dirty_price_kernel(mat_date, coup_rate, coup_freq, set_date, ytm):
    mat_date = d_to_ymd(mat_date)
    set_date = d_to_ymd(set_date)
    pcd = b_pre_coup_date_kernel(mat_date, coup_freq, set_date)
    ncd = b_next_coup_date_kernel(mat_date, coup_freq, set_date)
    c = coup_rate
    y = ytm / 100
    if coup_freq == 0:
        f = 1
    else:
        f = coup_freq
    n = b_count_coups_kernel(mat_date, coup_freq, set_date)
    if n == 1:
        t = (ncd - set_date) / (ncd - datetime(ncd.year - 1, ncd.month, ncd.day))
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
        return datetime(int('20' + fut_code[2:4]), int(fut_code[4:6]), 1)
    else:
        return datetime(int('20' + fut_code[1:3]), int(fut_code[3:5]), 1)


def f_final_listed_date(fut_code, the_nth_bus_day_following=0):
    rolling_day, num_fridays = f_first_date_of_deliv_month(fut_code), 0

    while num_fridays < 2:
        if rolling_day.weekday() == 4:
            num_fridays += 1
        rolling_day += timedelta(days=1)
    fld = rolling_day - timedelta(days=1)
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
    cf = f_conversion_factor_kernel(mat_date, coup_rate, coup_freq, fut_code)
    dp = b_dirty_price_kernel(mat_date, coup_rate, coup_freq, set_date, ytm)
    ai = b_accr_int_kernel(mat_date, coup_rate, coup_freq, matching_date)
    ncd = b_next_coup_date_kernel(mat_date, coup_freq, set_date)
    c = coup_rate
    f = coup_freq
    sum_coups_1 = 0
    sum_coups_2 = 0
    while ncd < matching_date:
        sum_coups_1 = sum_coups_1 + c / f
        sum_coups_2 = sum_coups_2 + c / f * (matching_date - ncd).days
        ncd = b_next_coup_date_kernel(mat_date, coup_freq, ncd)
    irr = (fut_price * cf + ai + sum_coups_1 - dp) * 365 / (dp * (matching_date - set_date).days - sum_coups_2) * 100
    return round(irr, 4)


def f_implied_repo_rate(bond_code, set_date, ytm, fut_code, fut_price, matching_date):
    set_date = d_to_ymd(set_date)
    matching_date = d_to_ymd(matching_date)
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    return f_implied_repo_rate_kernel(mat_date, coup_rate, coup_freq, set_date, ytm, fut_code, fut_price, matching_date)


def f_implied_yield(fut_code, fut_price, bond_code, matching_date=None):
    if matching_date is None:
        matching_date = f_final_listed_date(fut_code, the_nth_bus_day_following=3)
    [[bond_code, bond_name, value_date, mat_date, coup_rate, coup_freq]] \
        = bond_info[bond_info['bond_code'] == bond_code].values
    cf = f_conversion_factor(bond_code, fut_code, virtual_coup_rate=3)
    clean_price = fut_price * cf
    return b_yield(bond_code, matching_date, clean_price)


cnn = pymysql.connect(host='192.168.119.53', user='zhongyz', passwd='zyz2023!', db='bond', charset='utf8')

cur = cnn.cursor()

cur.execute("SELECT * FROM bond.pub_holidays ORDER BY dates")
rst = cur.fetchall()
pub_holidays = pd.DataFrame.from_records(list(rst), columns=['dates']).dates.map(lambda x:d_to_ymd(x)).tolist()

cur.execute("SELECT * FROM bond.bond_info ORDER BY BondCode")
rst = cur.fetchall()
bond_info = pd.DataFrame.from_records(list(rst), columns=['bond_code', 'bond_name', 'value_date', 'mat_date', 'coup_rate', 'coup_freq'])

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
    # start_date = "2026-02-13"
    # end_date = "2026-02-23"
    # s_dt = d_to_ymd(start_date)
    # e_dt = d_to_ymd(end_date)  
    # num = d_count_bus_days(s_dt, e_dt) - 1
    # target_str = f"T-{num}"
    # print(f"开始日期：{start_date}，结束日期：{end_date}，目标字符串：{target_str}")


