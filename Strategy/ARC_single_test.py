from self_finance_database_tool import *
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from math import sqrt
import sys
from .strategy_interface import *

class ARC_single_ticker(Strategy):
    ''' 
       將ARC策略應用在單一個股上，以觀察數據。
    ''' 

    def reset(self):
        self.strategy_name = "雙邊ARC"
        self.ema_days = 60
        self.short_days = 22
        self.long_days = 60
        self.adjust_window_days = 10
        self.vol_rolling_quantile = 0.8

        '''預設權重為等權重，TLT預設為不配置'''
        ticker_list = self.universe_data_df_dict["adjclose"].columns.to_list()
        target_weight_dict = dict()
        target_vol_dict = dict()
        self.single_ticker = ticker_list[0]
        
        target_weight_dict = {
            self.single_ticker:1,
        }
        
        self.original_weight_series = pd.Series(target_weight_dict)
    
    def cal_weight(self):
        changeDate_list = self._get_change_tradeDate_list(self.freq)
        price_df = self.universe_data_df_dict["adjclose"].shift(1)
        
        #去掉TLT的價格資料
        if "TLT" in price_df.columns:
            price_df.drop(columns=["TLT"], inplace=True)

        price_df.dropna(axis=0, inplace=True)

        #Universe的標的列表（已去除TLT）
        ticker_list = price_df.columns.to_list()

        #計算每日百分比變動
        percent_df = price_df.pct_change()
        
        #過去一年的年化波動df
        rolling_yr1_vol = percent_df.rolling(252).std() * sqrt(252)
        percentile_80_vol_df = rolling_yr1_vol.rolling(252).quantile(self.vol_rolling_quantile)
        
        #計算ema均線的df
        ema_df = price_df.ewm(span=self.ema_days).mean()
        ema_df.dropna(axis=0, inplace=True)
        
        #判斷每日收盤價格是否高於ema均線
        bull_bear_df = (price_df > ema_df)
        
        #取長短天期年化波動度較大者，作為近日波動度
        long_vol_df = percent_df.rolling(self.long_days, min_periods=1).std() * sqrt(252)
        short_vol_df = percent_df.rolling(self.short_days, min_periods=1).std() * sqrt(252)
        recent_max_vol_df = pd.concat([short_vol_df, long_vol_df]).max(level=0)

        weight_df = pd.DataFrame(index=changeDate_list, columns=ticker_list)
        target_vol_df = pd.DataFrame(index=changeDate_list, columns=ticker_list)
        bb_count_series_list = list()
    
        for index, date in enumerate(changeDate_list, 1):
            #顯示權重計算進度
            percentage = 100*index/len(changeDate_list)
            sys.stdout.write('\r'+"權重計算：連續訊號計算完成度{percentage:.2f}%".format(percentage=percentage))
            
            bull_ticker_dict = dict()
            bear_ticker_dict = dict()

            for ticker in ticker_list:
                bull_bear_count = 0 #紀錄回顧過去一段期間的強弱勢程度
                day_count = 0 #紀錄已回顧幾天
                cur_date = date #回顧過程中的當前日期 
                
                #若換倉日前一日收盤價在ema均線以上，判斷為強勢股，反之為弱勢股，以flag作為標記
                bull_bear_flag = bull_bear_df.at[date, ticker]
                
                #避免回推的日期超過邊界（資料日首日）而報錯
                while (day_count < self.adjust_window_days and cur_date > bull_bear_df.index[0]):
                    if bull_bear_df.at[cur_date, ticker] == bull_bear_flag:
                        bull_bear_count +=1
                        day_count +=1
                        cur_date = self.db.get_last_tradeDate(cur_date - timedelta(days=1))

                    else:
                        break

                #寫入趨勢連續天數
                if bull_bear_flag == True:
                    bull_ticker_dict[ticker] = bull_bear_count
                
                elif bull_bear_flag == False:
                    bear_ticker_dict[ticker] = bull_bear_count

            bb_count_series_list.append(bull_bear_count)

            bull_count_series = pd.Series(bull_ticker_dict)
            bear_count_series = pd.Series(bear_ticker_dict)
            
            bull_ticker_list = list(bull_ticker_dict.keys())
            bear_ticker_list = list(bear_ticker_dict.keys())

            original_target_vol_series = percentile_80_vol_df.loc[date,:]
            
            #取得預設權重
            bull_original_weight_series = self.original_weight_series.loc[bull_ticker_list]
            bear_original_weight_series = self.original_weight_series.loc[bear_ticker_list]

            #將原目標波動依趨勢天數調整成真實的目標波動
            bull_target_vol_series = original_target_vol_series.loc[bull_ticker_list]
            bull_target_vol_series = bull_target_vol_series * bull_original_weight_series
            bull_target_vol_series = bull_target_vol_series.multiply(1+(bull_count_series/self.adjust_window_days))

            bear_target_vol_series = original_target_vol_series.loc[bear_ticker_list]
            bear_target_vol_series = bear_target_vol_series * bear_original_weight_series
            bear_target_vol_series = bear_target_vol_series.multiply(1-(bear_count_series/self.adjust_window_days))
            bear_target_vol_series[bear_target_vol_series < 0] = 0

            #將真實目標波動除於近日波動得到原始策略權重
            bull_raw_target_weight_series = bull_target_vol_series.div(recent_max_vol_df.loc[date,bull_ticker_list])            
            bear_raw_target_weight_series = bear_target_vol_series.div(recent_max_vol_df.loc[date,bear_ticker_list])
            
            #將原始策略權重與預設權重對比，強勢股取大者，弱勢股取小者
            bull_target_weight_series = pd.concat([bull_raw_target_weight_series, bull_original_weight_series], sort=True, axis=1).max(axis=1)
            bear_target_weight_series = pd.concat([bear_raw_target_weight_series, bear_original_weight_series], sort=True, axis=1).min(axis=1)
            
            #將權重和調整至小於等於1
            total_weight = bull_target_weight_series.sum() + bear_target_weight_series.sum()
            
            if total_weight > 1:
                bull_target_weight_series = bull_target_weight_series / total_weight
                bear_target_weight_series = bear_target_weight_series / total_weight

            weight_df.loc[date, bull_ticker_list] = bull_target_weight_series
            weight_df.loc[date, bear_ticker_list] = bear_target_weight_series
            
            target_vol_df.loc[date, bull_ticker_list] = bull_target_vol_series
            target_vol_df.loc[date, bear_ticker_list] = bear_target_vol_series
        
        file_name = "ARC對{ticker}個股測試數據.xlsx".format(ticker = self.single_ticker)
        ARC_excel = pd.ExcelWriter(file_name,engine='xlsxwriter')   # Creating Excel Writer Object from Pandas    
        
        #儲存成Excel檔
        price_df.to_excel(ARC_excel, sheet_name='實際價格')
        ema_df.to_excel(ARC_excel, sheet_name='EMA均線')
        bull_bear_df.to_excel(ARC_excel, sheet_name='強弱勢判斷')
        long_vol_df.to_excel(ARC_excel, sheet_name='近期波動（長期）')
        short_vol_df.to_excel(ARC_excel, sheet_name='近期波動（短期）')
        recent_max_vol_df.to_excel(ARC_excel, sheet_name='實際波動')
        rolling_yr1_vol.to_excel(ARC_excel, sheet_name='近一年歷史波動')
        percentile_80_vol_df.to_excel(ARC_excel, sheet_name='波動度80百分位')
        bb_count_df = pd.DataFrame(bb_count_series_list, index=changeDate_list)
        bb_count_df.to_excel(ARC_excel, sheet_name='連續天數')
        target_vol_df.to_excel(ARC_excel, sheet_name='目標波動')
        weight_df.to_excel(ARC_excel, sheet_name='權重')
        ARC_excel.save()

        weight_df["TLT"] = 1-weight_df.sum(axis=1)
        return weight_df