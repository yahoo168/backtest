import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from math import sqrt
import matplotlib.pyplot as plt

def makeFolder(folder_name):
    if not os.path.isdir(folder_name):
        os.makedirs(folder_name)

    else:
        return None

def find_backtest_record(strategy_name, universe_name, start_date, end_date, data_type_list=[]):
    s_date = ''.join(start_date.split("-"))
    e_date = ''.join(end_date.split("-"))
    
    data_df_dict = {}
    for data_type in data_type_list:
        filename = '_'.join([universe_name, data_type, s_date, e_date]) + ".xlsx"
        filepath = os.path.join(strategy_name, filename)
        print(filepath)
        data_df = pd.read_excel(filepath, index_col=0)
        data_df_dict[data_type] = data_df
    
    return data_df_dict

#將調倉頻率由文字轉換至數字
def freq_to_days(freq):
    if freq == "daily":
        period = 1

    elif freq == "week":
        period = 5

    elif freq == "month":
        period = 22

    elif freq == "quarter":
        period = 60

    elif freq == "year":
        period = 252

    return period

#計算平均手續費
def cal_avg_commision(fee_series, freq):
    freq_days = freq_to_days(freq)
    mean_fee = fee_series.mean()
    return mean_fee * (252/freq_days)

#計算基本績效指標（平均報酬 / 平均波動 / 平均夏普）
def cal_basic_performance(sum_percent_series, period=252):
    #計算rolling平均報酬
    return_rolling = (1+sum_percent_series).rolling(period).apply(np.prod, raw=True) - 1
    #計算rolling平均波動率
    volatility_rolling = sum_percent_series.rolling(period).std() * sqrt(252)
    #計算平均夏普ratio
    sharpe_rolling = return_rolling / volatility_rolling
    # print(return_rolling)
    avg_return = str(round(100*return_rolling.mean(),2))+" %"
    avg_volatility = str(round(100*volatility_rolling.mean(),2))+" %"
    avg_sharpe = round(sharpe_rolling.mean(),2)

    return avg_return, avg_volatility, avg_sharpe

#計算MDD，回傳MDD與 起始日 / 結束日
def cal_maxdrawdown(value_series):
    #紀錄近期資產價值最高的日期，預設為第一日
    peak_date = value_series.index[0]
    #紀錄近期資產價值最高的數額，預設為1
    peak_value = 1
    #紀錄MDD期間的起始日與結束日
    MDD_s_date = value_series.index[0]
    MDD_e_date = value_series.index[0]
    MDD = 0

    for date, v in value_series.iteritems():
        #若價值突破前波高點，則更新高點日期與高點價值
        if v >= peak_value:
            peak_date = date
            peak_value = v
        
        #反之則紀錄期間內最大跌幅
        else:
            drawdown = (peak_value - v) / peak_value
            if drawdown >= MDD:
                MDD_s_date = peak_date
                MDD_e_date = date
                MDD = drawdown

    return(round(MDD,2), (MDD_s_date, MDD_e_date))

#計算勝率，每日比對策略與大盤的百分比變動高低
def cal_win_rate(sum_percent_series, benchmark_percent_series):
    win_series = sum_percent_series > benchmark_percent_series
    win_rate = round((win_series.sum() / len(win_series)), 2)
    cum_win_series = (sum_percent_series - benchmark_percent_series)
    return win_rate

#計算夏普勝率，每日比對策略與大盤的夏普值高低
def cal_sharpe_win_rate(sum_percent_series, benchmark_percent_series, period=252):
    #計算rolling平均報酬
    sum_return_rolling = (1+sum_percent_series).rolling(period).apply(np.prod, raw=True) - 1
    #計算rolling平均波動率
    sum_volatility_rolling = sum_percent_series.rolling(period).std() * sqrt(252)
    #計算平均夏普ratio
    sum_sharpe_rolling = sum_return_rolling / sum_volatility_rolling

    #計算rolling平均報酬
    benchmark_return_rolling = (1+benchmark_percent_series).rolling(period).apply(np.prod, raw=True) - 1
    #計算rolling平均波動率
    benchmark_volatility_rolling = benchmark_percent_series.rolling(period).std() * sqrt(252)
    #計算平均夏普ratio
    benchmark_sharpe_rolling = benchmark_return_rolling / benchmark_volatility_rolling

    sharpe_win_rolling = sum_sharpe_rolling > benchmark_sharpe_rolling 
    sharpe_win_days = sharpe_win_rolling.sum()
    sharpe_win_rate = sharpe_win_days / len(sharpe_win_rolling)
    
    return round(sharpe_win_rate, 2)

#計算分年績效
def cal_performance_per_yr(sum_percent_series, benchmark_percent_series):
    #先取得資產組合的分年序列
    dt = sum_percent_series
    sum_percent_series_list = [dt[dt.index.year == y] for y in dt.index.year.unique()]    
    
    #取得Benchmark的分季序列
    dt_2 = benchmark_percent_series
    benchmark_percent_series_list = [dt_2[dt_2.index.year == y] for y in dt_2.index.year.unique()]
    
    #紀錄分年績效
    perfomance_per_yr_df = pd.DataFrame()
    for index, percent_series in enumerate(sum_percent_series_list,0):
        #以各年序列第一筆資料的年份作為本序列年份代表
        yr = percent_series.index[0].year
        
        #使用原先計算全區間的績效指標函數，將period設為分段資料長度，
        #則只有最後一日的rolling績效指標有值，其他日為Nan
        yr_return, yr_std, yr_sharpe = cal_basic_performance(percent_series, period=len(percent_series))
        
        #由百分比序列構建價值序列，以便計算績效
        value_series = (1+percent_series).cumprod()
        MDD, date_interval = cal_maxdrawdown(value_series)
        MDD_s_date =  datetime.strftime(date_interval[0], "%Y-%m-%d")
        MDD_e_date =  datetime.strftime(date_interval[1], "%Y-%m-%d")
        win_rate = cal_win_rate(percent_series, benchmark_percent_series_list[index])
        
        perfomance_per_yr_df.loc[yr, "Return"] = yr_return
        perfomance_per_yr_df.loc[yr, "Std"] = yr_std
        perfomance_per_yr_df.loc[yr, "Sharpe"] = yr_sharpe
        perfomance_per_yr_df.loc[yr, "MaxDrawdown"] = MDD
        perfomance_per_yr_df.loc[yr, "MDD_s_date"] = MDD_s_date
        perfomance_per_yr_df.loc[yr, "MDD_e_date"] = MDD_e_date
        perfomance_per_yr_df.loc[yr, "Win Rate"] = win_rate
    
    return perfomance_per_yr_df

#將單年的百分比序列切分為分季的百分比序列（依月份區分）
#傳入值為一個list，其中含有分年的百分比變動序列。
def yr_to_quarter_series(yr_series_list):
    quarter_series_list = list()
    for series_yr in yr_series_list:
        Qtr_1_series = series_yr[(series_yr.index.month > 0)&(series_yr.index.month <= 3)]
        Qtr_2_series = series_yr[(series_yr.index.month > 3)&(series_yr.index.month <= 6)]
        Qtr_3_series = series_yr[(series_yr.index.month > 6)&(series_yr.index.month <= 9)]
        Qtr_4_series = series_yr[(series_yr.index.month > 9)&(series_yr.index.month <= 12)]
        
        quarter_series_list.append(Qtr_1_series)
        quarter_series_list.append(Qtr_2_series)
        quarter_series_list.append(Qtr_3_series)
        quarter_series_list.append(Qtr_4_series)
    
    return quarter_series_list

#計算分季績效
def cal_performance_per_quarter(sum_percent_series, benchmark_percent_series):
    dt = sum_percent_series
    #先取得資產組合的分年序列
    sum_percent_yr_series_list = [dt[dt.index.year == y] for y in dt.index.year.unique()]
    #取得分季序列
    sum_percent_quarter_series_list = yr_to_quarter_series(sum_percent_yr_series_list)
    #去除分季序列的空值，因頭尾兩年可能沒有完整的分季資料
    sum_percent_quarter_series_list = [x for x in sum_percent_quarter_series_list if not x.empty]
    
    #方法同上，取得Benchmark的分季序列
    dt_2 = benchmark_percent_series
    benchmark_percent_yr_series_list = [dt_2[dt_2.index.year == y] for y in dt_2.index.year.unique()]
    benchmark_percent_quarter_series_list = yr_to_quarter_series(benchmark_percent_yr_series_list)
    benchmark_percent_quarter_series_list = [x for x in benchmark_percent_quarter_series_list if not x.empty]

    #紀錄分季績效的DataFrame
    perfomance_per_Qr_df = pd.DataFrame()
    for index, percent_series in enumerate(sum_percent_quarter_series_list, 0):
        #以分季序列第一日的季度代號作為該季的代表（e.g:2021Q1）
        Qr = pd.Period(percent_series.index[0], 'Q').__str__()
        
        #計算分季績效的方式與分年績效相同
        Qr_return, Qr_std, Qr_sharpe = cal_basic_performance(percent_series, period=len(percent_series))
        value_series = (1+percent_series).cumprod()
        MDD, date_interval = cal_maxdrawdown(value_series)
        MDD_s_date = datetime.strftime(date_interval[0], "%Y-%m-%d")
        MDD_e_date = datetime.strftime(date_interval[1], "%Y-%m-%d")
        win_rate = cal_win_rate(percent_series, benchmark_percent_quarter_series_list[index])
        
        #填入分季績效指標
        perfomance_per_Qr_df.loc[Qr, "Return"] = Qr_return
        perfomance_per_Qr_df.loc[Qr, "Std"] = Qr_std
        perfomance_per_Qr_df.loc[Qr, "Sharpe"] = Qr_sharpe
        perfomance_per_Qr_df.loc[Qr, "MaxDrawdown"] = MDD
        perfomance_per_Qr_df.loc[Qr, "MDD_s_date"] = MDD_s_date
        perfomance_per_Qr_df.loc[Qr, "MDD_e_date"] = MDD_e_date
        perfomance_per_Qr_df.loc[Qr, "Win Rate"] = win_rate
    
    return perfomance_per_Qr_df

#計算調倉區間內，個別資產對整體組合變動的貢獻佔比（有可能超過100%）
def cal_profit_ratio(percent_df=None, freq="month"):
    period = freq_to_days(freq)
    sum_percent_series = percent_df.sum(axis=1) 
    rolling_percent_df = percent_df.rolling(period).mean()
    ratio_df = rolling_percent_df.div(sum_percent_series, axis=0)
    ratio_df.dropna(axis=0, inplace=True)
    
    #部分極端值會嚴重影響圖表呈現，限制極端值範圍後再展示
    de_extreme_ratio_df = ratio_df.copy()
    de_extreme_ratio_df[de_extreme_ratio_df > 1] = 1
    de_extreme_ratio_df[de_extreme_ratio_df < -1] = -1
    
    return de_extreme_ratio_df, ratio_df

# 待改：每年重新執行的策略績效～～

# perfomance_per_yr_df = pd.DataFrame(index=range(2008, 2020), columns=["Return", "Std", "Sharpe", "MaxDrawdown"])
# for yr in range(2008, 2021):
#     op = options.copy()
#     start_date = str(yr)+"-01-01"
#     end_date = str(yr)+"-12-18"
#     op["start_date"] = start_date
#     op["end_date"] = end_date
#     ARC = Backtest(ARC_basic, universe_ticker_list, op)
#     ARC.activate()
    
#     start_value = end_value = ARC.sum_value_series[0]
#     end_value = ARC.sum_value_series[-1]
#     yr_return = (end_value-start_value) / start_value
#     yr_std = ARC.sum_value_series.pct_change().std()*sqrt(252)
#     yr_sharpe = yr_return / yr_std
#     MDD, date_interval = cal_maxdrawdown(ARC.sum_value_series)
#     perfomance_per_yr_df.loc[yr, "Return"] = yr_return
#     perfomance_per_yr_df.loc[yr, "Std"] = yr_std
#     perfomance_per_yr_df.loc[yr, "Sharpe"] = yr_sharpe
#     perfomance_per_yr_df.loc[yr, "MaxDrawdown"] = MDD

# print(perfomance_per_yr_df)
# perfomance_per_yr_df.to_excel("yr_perfomance_2020.xlsx")