

# ======================= 导入依赖 =======================
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import re
import sys

# ======================= 模块一：从 Baostock 获取低PE股票 =======================

def get_all_a_stock_codes():
    """获取全量A股代码（含沪深主板/创业板/科创板/北交所）"""
    rs = bs.query_stock_basic()
    codes = []
    while (rs.error_code == '0') and rs.next():
        code = rs.get_row_data()[0]
        # 匹配所有A股（sh.60, sh.68, sh.00, sz.30, sz.002, sh.688, bj.43, bj.83, bj.87, bj.92）
        if any(code.startswith(prefix) for prefix in [
            'sh.60', 'sh.68', 'sz.00', 'sz.30', 'sz.002', 'sh.688',  # 沪深市场
            'bj.43', 'bj.83', 'bj.87', 'bj.92'  # 北交所新增支持
        ]):
            codes.append(code)
    print(f"Successfully fetched {len(codes)} A-share stock codes (including STAR Market and Beijing Stock Exchange)")
    return codes

def get_real_trade_date():
    """智能获取最近有效交易日（自动校正节假日）"""
    now = datetime.now()
    # 扩展查询范围到30天（覆盖春节等长假）
    rs = bs.query_trade_dates(
        start_date=(now - timedelta(days=30)).strftime('%Y-%m-%d'),
        end_date=now.strftime('%Y-%m-%d'))

    trade_dates = []
    while (rs.error_code == '0') and rs.next():
        date_info = rs.get_row_data()
        if date_info[1] == '1':  # 交易日
            trade_dates.append(date_info[0])

    if not trade_dates:
        return now.strftime('%Y-%m-%d')

    today_str = now.strftime('%Y-%m-%d')
    last_date = trade_dates[-1]

    # 核心判断逻辑
    if today_str not in trade_dates:  # 今天非交易日
        print(f"Warning: Current date {today_str} is not a trading day, using last trading day: {last_date}")
        return last_date
    elif now.hour < 15:  # 当天未收盘
        prev_date = trade_dates[-2] if len(trade_dates) >= 2 else last_date
        print(f"Info: Current time {now.strftime('%H:%M')} is before market close, using previous trading day: {prev_date}")
        return prev_date
    else:  # 当天已收盘
        print(f"Successfully using today's closing data: {today_str}")
        return today_str

def get_pe_stocks():
    """
    使用 baostock 获取全量A股最新市盈率，筛选出市盈率(PE)在0到30之间的股票。
    保存结果到Excel文件，并返回股票代码列表。
    """
    print("步骤 1: 开始使用 baostock 获取低PE股票...")

    # 获取关键参数
    trade_date = get_real_trade_date()
    all_codes = get_all_a_stock_codes()

    # 数据存储
    pe_data = {}
    start_time = time.time()
    failed_codes = []

    # 批量获取
    for i, code in enumerate(all_codes, 1):
        try:
            rs = bs.query_history_k_data_plus(
                code=code,
                fields="peTTM",
                start_date=trade_date,
                end_date=trade_date,
                frequency="d",
                adjustflag="3")

            if rs.data and rs.data[0][0]:
                pe_value = round(float(rs.data[0][0]), 2)
                # 只保留0 < PE <= 30的数据
                if 0 < pe_value <= 30:
                    pe_data[code] = pe_value

            # 进度显示
            if i % 600 == 0:
                elapsed = time.time() - start_time
                print(f"Progress: {i}/{len(all_codes)} ({i/len(all_codes):.1%}) | Elapsed: {elapsed:.1f}s")

        except Exception as e:
            failed_codes.append(code)
            continue

    # 生成DataFrame
    df = pd.DataFrame.from_dict(
        pe_data,
        orient='index',
        columns=[f"PE_{trade_date}"]
    )
    df.index.name = "股票代码"

    # 保存文件
    output_filename = "pe_selected_stocks.xlsx"
    df.to_excel(output_filename, float_format="%.2f")

    print(f"低PE股票列表已保存至: {output_filename}")
    print(f"发现 {len(pe_data)} 只满足条件的股票。")

    return df.index.tolist(), output_filename

# ======================= 模块二：从 Baostock 下载股票数据 =======================

def normalize_stock_code(code):
    """
    将各种格式的股票代码转换为baostock标准格式(sh.600000/sz.000001/bj.430718)
    """
    if pd.isna(code) or not isinstance(code, (str, int, float)):
        return None
    
    code = str(code).strip()
    
    if re.match(r'^(sh|sz|bj)\.[0-9]{6}$', code):
        return code
    
    # 确保代码是6位数字
    code = re.sub(r'[^0-9]', '', code).zfill(6)
    
    if len(code) != 6:
        return None

    if code.startswith(('6', '9')) or code.startswith('68'):
        return f'sh.{code}'
    elif code.startswith(('0', '2', '3')):
        return f'sz.{code}'
    elif code.startswith(('43', '83', '87', '92')):
        return f'bj.{code}'
    
    return None

def get_recent_trade_days(days=60):
    """
    获取最近N个交易日日期列表
    """
    # 扩展查询日期范围以确保获取足够数据
    rs = bs.query_trade_dates(
        start_date=(datetime.now() - pd.DateOffset(days=120)).strftime('%Y-%m-%d'),
        end_date=datetime.now().strftime('%Y-%m-%d'))
    trade_days = []
    while (rs.error_code == '0') and rs.next():
        date_info = rs.get_row_data()
        if date_info[1] == '1':
            trade_days.append(date_info[0])
    return sorted(trade_days, reverse=True)[:days]

def download_stock_data(codes):
    """
    根据提供的股票代码列表，使用 baostock 下载历史数据并保存到Excel。
    """
    print("\n步骤 2: 开始使用 baostock 下载指定股票的详细数据...")
    if not codes:
        print("没有提供股票代码，跳过下载。")
        return None

    # 标准化股票代码
    normalized_codes = [normalize_stock_code(c) for c in codes]
    normalized_codes = [c for c in normalized_codes if c is not None]
    
    if not normalized_codes:
        print("所有提供的股票代码都无效，无法下载数据。")
        return None
        
    print(f"准备下载 {len(normalized_codes)} 只股票的数据...")

    trade_days = get_recent_trade_days(60)
    if not trade_days:
        print("无法获取最近交易日，无法继续。")
        return None

    data_dict = {code: {} for code in normalized_codes}
    start_time = time.time()

    for i, code in enumerate(normalized_codes, 1):
        try:
            rs = bs.query_history_k_data_plus(
                code=code,
                fields="date,open,high,low,close,pctChg,peTTM,amount",
                start_date=trade_days[-1],
                end_date=trade_days[0],
                frequency="d",
                adjustflag="3")
            df = rs.get_data()
            
            for _, row in df.iterrows():
                date = row['date']
                try:
                    data_dict[code][f"开盘价_{date}"] = round(float(row['open']), 2) if row['open'] else np.nan
                    data_dict[code][f"最高价_{date}"] = round(float(row['high']), 2) if row['high'] else np.nan
                    data_dict[code][f"最低价_{date}"] = round(float(row['low']), 2) if row['low'] else np.nan
                    data_dict[code][f"收盘价_{date}"] = round(float(row['close']), 2) if row['close'] else np.nan
                    data_dict[code][f"涨幅_{date}"] = round(float(row['pctChg']), 2) if row['pctChg'] else np.nan
                    data_dict[code][f"市盈率_{date}"] = round(float(row['peTTM']), 1) if row['peTTM'] else np.nan
                    data_dict[code][f"成交额(亿元)_{date}"] = round(float(row['amount'])/1e8, 3) if row['amount'] else np.nan
                except (ValueError, TypeError):
                    pass

            if i % 100 == 0 or i == len(normalized_codes):
                elapsed = time.time() - start_time
                print(f"下载进度: {i}/{len(normalized_codes)} ({i/len(normalized_codes):.1%}) | 用时: {elapsed:.1f}秒")
        except Exception as e:
            print(f"× 获取 {code} 数据失败: {e}")
            continue
    
    result_df = pd.DataFrame.from_dict(data_dict, orient='index').apply(pd.to_numeric, errors='coerce')
    result_df.index.name = "股票代码"
    
    output_filename = "stock_kline_data.xlsx"
    result_df.to_excel(output_filename, float_format="%.3f")
    print(f"\n详细数据已保存至: {output_filename}")
    return output_filename

# ======================= 主程序入口 =======================

if __name__ == "__main__":
    # 步骤1：获取低PE股票
    pe_stock_codes, pe_filename = get_pe_stocks()
    
    # 如果成功获取到股票代码，则继续下一步
    if pe_stock_codes:
        # 步骤2：登录BaoStock并下载数据
        lg = bs.login()
        if lg.error_code != '0':
            print(f"Baostock 登录失败: {lg.error_msg}")
            sys.exit(1)
        
        try:
            download_stock_data(pe_stock_codes)
        finally:
            bs.logout()
            print("Baostock 已登出。")
    
    print("\n所有任务完成。")

