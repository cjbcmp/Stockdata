# ======================= 导入依赖 =======================
import baostock as bs
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
import re
import sys

# ======================= 辅助函数 =======================

def get_all_a_stock_codes_data_py():
    """获取全量A股股票代码列表（包含上证、深证、创业板、科创板、北交所等） - 来自BS data.py"""
    rs = bs.query_stock_basic()  # 查询股票基本信息
    stock_list = []
    while (rs.error_code == '0') & rs.next():
        stock_list.append(rs.get_row_data())
    df = pd.DataFrame(stock_list, columns=rs.fields)
    # 使用正则表达式筛选A股代码（60/68开头为上证，00/30/002开头为深证，688开头为科创板，43/83/87/92开头为北交所）
    a_shares = df[df['code'].str.match(r'(sh.60|sh.68|sh.688|sz.00|sz.30|sz.002|bj.43|bj.83|bj.87|bj.92)')]
    return a_shares['code'].tolist()

def get_real_trade_date_pe_py():
    """智能获取最近有效交易日（自动校正节假日） - 来自BS pe.py"""
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
        print(f"当前日期 {today_str} 非交易日，使用最近交易日: {last_date}")
        return last_date
    elif now.hour < 15:  # 当天未收盘
        prev_date = trade_dates[-2] if len(trade_dates) >= 2 else last_date
        print(f"当前时间 {now.strftime('%H:%M')} 未收盘，使用上一交易日: {prev_date}")
        return prev_date
    else:  # 当天已收盘
        print(f" 使用当天收盘数据: {today_str}")
        return today_str

def get_recent_trade_days_data_py(days=60):
    """获取最近N个交易日日期列表 - 来自BS data.py"""
    # 扩展查询日期范围以确保获取足够数据
    rs = bs.query_trade_dates(
        start_date=(datetime.now() - pd.DateOffset(days=120)).strftime('%Y-%m-%d'),
        end_date=datetime.now().strftime('%Y-%m-%d'))
    trade_days = []
    while (rs.error_code == '0') and rs.next():
        date_info = rs.get_row_data()
        if date_info[1] == '1':  # 筛选交易日标识为1的日期
            trade_days.append(date_info[0])
    return sorted(trade_days, reverse=True)[:days]  # 返回最近的N个交易日

def normalize_stock_code_data_py(code):
    """
    标准化股票代码格式 - 来自BS data.py
    将各种格式的股票代码转换为baostock标准格式(sh.600000/sz.000001/bj.430718)
    特别注意保留前导零(如000001)
    """
    if pd.isna(code) or not isinstance(code, (str, int, float)):
        return None
    
    # 转换为字符串并处理
    code = str(code).strip().upper()
    
    # 如果已经是标准格式(sh.600000/sz.000001/bj.430718)
    if re.match(r'^(sh|sz|bj)\.[0-9]{6}$', code):
        return code
    
    # 移除可能的前缀(SH/SZ/BJ)或后缀(.SH/.SZ/.BJ)
    code = re.sub(r'^(SH|SZ|BJ)?\.?', '', code)
    code = re.sub(r'\.(SH|SZ|BJ)?$', '', code)
    
    # 只保留数字部分，并确保长度为6位(不足前面补零)
    code = re.sub(r'[^0-9]', '', code)
    code = code.zfill(6)  # 确保6位长度，不足前面补零
    
    # 根据数字代码判断市场
    if len(code) == 6:
        if code.startswith(('6', '9', '68')):  # 上证(6/9开头)和科创板(688开头)
            return f'sh.{code}'
        elif code.startswith(('0', '2', '3')):  # 深证(0/2/3开头)
            return f'sz.{code}'
        elif code.startswith(('43', '83', '87', '92')):  # 北交所(43/83/87/92开头)
            return f'bj.{code}'
    
    return None

# ======================= 模块一：从 Baostock 获取低PE股票 (改编自BS pe.py) =======================

def get_pe_filtered_stock_codes():
    """
    使用 baostock 获取全量A股最新市盈率，筛选出市盈率(PE)在0到30之间的股票。
    保存结果到Excel文件，并返回股票代码列表。
    """
    print("\n" + "="*50)
    print("  步骤1: 获取低PE股票列表")
    print("="*50 + "\n")
    
    # 获取关键参数
    trade_date = get_real_trade_date_pe_py()
    all_codes = get_all_a_stock_codes_data_py() # Using data.py's get_all_a_stock_codes
    
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
                if 0 < pe_value <= 30:
                    pe_data[code] = pe_value
            
            if i % 300 == 0: # Original BS pe.py uses 300 for progress
                elapsed = time.time() - start_time
                remain = (len(all_codes) - i) * (elapsed / i)
                print(f" 进度: {i}/{len(all_codes)} ({i/len(all_codes):.1%}) | 已用: {elapsed:.1f}s | 剩余: {remain:.1f}s")
                
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
    filename = "pe_selected_stocks.xlsx" # Fixed filename
    df.to_excel(filename, float_format="%.2f")
    
    print(f"采集完成！共成功获取 {len(pe_data)} 只股票数据（0 < PE ≤ 30）")
    print(f"文件保存至: {filename}")
    print(f"总耗时: {time.time() - start_time:.1f}秒")
    if failed_codes:
        print(f"失败代码数: {len(failed_codes)} (示例: {failed_codes[:5]}...)")
    
    return df.index.tolist(), filename

# ======================= 模块二：从 Baostock 下载股票数据 (改编自BS data.py) =======================

def fetch_stock_data_kline(codes):
    """
    主数据获取函数：获取股票K线数据并保存到Excel
    :param codes: 股票代码列表
    """
    print("\n" + "="*50)
    print("  步骤2: 下载股票K线数据")
    print("="*50 + "\n")
    
    trade_days = get_recent_trade_days_data_py(60)  # 获取最近60个交易日
    
    # 初始化数据存储字典（使用多层字典结构提升存储效率）
    data_dict = {code: {} for code in codes}
    start_time = time.time()
    
    # 分批获取股票数据
    for i, code in enumerate(codes, 1):
        try:
            rs = bs.query_history_k_data_plus(
                code=code,
                fields="date,open,high,low,close,pctChg,peTTM,amount",
                start_date=trade_days[-1],  # 最早日期
                end_date=trade_days[0],    # 最新日期
                frequency="d",             # 日线数据
                adjustflag="3")            # 前复权处理
            df = rs.get_data()
            
            # 将数据存入字典（按日期字段动态生成列名）
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
                except ValueError:
                    pass  # 忽略数据转换异常
            # 进度显示
            if i % 400 == 0 or i == len(codes):
                elapsed = time.time() - start_time
                print(f" 进度: {i}/{len(codes)} ({i/len(codes):.1%}) | 用时: {elapsed:.1f}秒")
        except Exception as e:
            print(f" 获取 {code} 数据失败: {str(e)}")
            continue
    
    # 转换为DataFrame并进行数据清洗
    result_df = pd.DataFrame.from_dict(data_dict, orient='index').apply(pd.to_numeric, errors='coerce')
    result_df.index.name = "股票代码"
    
    # 生成固定文件名
    filename = "stock_kline_data.xlsx" # Fixed filename
    result_df.to_excel(filename, float_format="%.3f")  # 保留3位小数
    print(f"数据已保存至: {filename}")
    
    return filename

# ======================= 主程序入口 =======================

if __name__ == "__main__":
    lg = None # Initialize lg outside try for finally block
    try:
        # 登录BaoStock
        lg = bs.login()
        if lg.error_code != '0':
            print(f"Baostock 登录失败: {lg.error_msg}")
            sys.exit(1) # Exit if login fails in non-loop mode

        # 步骤1：获取低PE股票
        pe_stock_codes, pe_filename = get_pe_filtered_stock_codes()

        # 如果成功获取到股票代码，则继续下一步
        if pe_stock_codes:
            # 步骤2：下载K线数据
            kline_filename = fetch_stock_data_kline(pe_stock_codes)
        else:
            print("未获取到符合条件的低PE股票，跳过K线数据下载。")

    except Exception as e:
        print(f"任务执行过程中发生未预期错误: {e}")

    finally:
        if lg:
            bs.logout()
            print(" Baostock 已登出。")

    print(f"\n{'='*60}\n本次任务执行完毕。\n{'='*60}")
