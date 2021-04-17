from self_finance_database_tool import *
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from math import sqrt
import sys
from .strategy_interface import *

class ARC_basic(Strategy):
    def reset(self):
        self.strategy_name = "ARC_basic"
        self.ema_days = 22
        self.short_days = 22
        self.long_days = 60
        self.STEP = 0.05
        self.target_weight_dict = dict()
        self.target_vol_dict = dict()
        
        ticker_list = self.universe_data_df_dict["adjclose"].columns.to_list()

        for ticker in ticker_list:
            if ticker != "TLT":
                self.target_weight_dict[ticker] = 1/(len(ticker_list)-1)
                self.target_vol_dict[ticker] = 0.3

    def cal_weight(self):
        changeDate_list = self._get_change_tradeDate_list(self.freq)
        price_df = self.universe_data_df_dict["adjclose"].shift(1)
        
        '''去掉TLT的價格資料'''
        price_df.drop(columns=["TLT"], inplace=True)
        price_df.dropna(axis=0, inplace=True)
        percent_df = price_df.pct_change()
        
        '''取得Universe的標的（已去除TLT）'''
        ticker_list = price_df.columns.to_list()
        
        '''計算ema均線的df'''
        ema_df = price_df.ewm(span=self.ema_days).mean()
        ema_df.dropna(axis=0, inplace=True)
        
        '''bull_bear_df：價格是否高於ema均線'''
        bull_bear_df = (price_df > ema_df)
        
        signal_df = pd.DataFrame(index=changeDate_list, columns=ticker_list)
        tradaDate_list = bull_bear_df.index
        # print(ticker_list)
        for index, date in enumerate(changeDate_list, 1):
            percentage = 100*index/len(changeDate_list)
            sys.stdout.write('\r'+"連續出訊計算完成度{percentage:.2f}%".format(percentage=percentage))
            
            for ticker in ticker_list:
                count = 0
                cur_date = date         
                while(True):
                    if cur_date >= tradaDate_list[0] and bull_bear_df.loc[cur_date, ticker] == True:
                        count +=1
                        cur_date = self.db.get_last_tradeDate(cur_date - timedelta(days=1))
                
                    else:
                        break

                signal_df.loc[date, ticker] = count
        step_minus_vol_df = signal_df * self.STEP
        long_vol_df = percent_df.rolling(self.long_days, min_periods=1).std() * sqrt(252)
        short_vol_df = percent_df.rolling(self.short_days, min_periods=1).std() * sqrt(252)
        
        changeDate_long_vol = long_vol_df.shift(1).loc[changeDate_list,:]
        changeDate_short_vol = short_vol_df.shift(1).loc[changeDate_list,:]     
        vol = pd.concat([changeDate_long_vol, changeDate_short_vol]).max(level=0)
        
        target_vol_df = pd.DataFrame(self.target_vol_dict, index=changeDate_list)
        re_target_vol_df = (target_vol_df - step_minus_vol_df) / len(ticker_list)
        re_target_vol_df[re_target_vol_df < 0] = 0
        
        prior_weight_df = pd.DataFrame(self.target_weight_dict, index=changeDate_list)
        raw_weight_df = re_target_vol_df / vol
        adjust_weight_df = pd.concat([prior_weight_df, raw_weight_df], sort=True).min(level=0)
        adjust_weight_df["TLT"] = 1 - adjust_weight_df.sum(axis=1)
        return adjust_weight_df

class ARC_bi_side_adjust(Strategy):
    ''' 
        考慮多空雙邊，若股價在EMA之上，
        則依其在EMA之上的連續日期增加目標曝險波動度，
        並取目標權重與預設權重較高者，若權重和超過1則比例調整至總和為1。
        若股價在EMA之下，則依原先作法，依連續日期降低目標曝險，
        並取目標權重與預設權重中較小者。
        增減日期採區間增減，以原預設波動的正負1倍作為變動區間，
        每連續一日則增加區間的固定比例（由參數調整）。
    ''' 

    def reset(self):
        self.strategy_name = "雙邊ARC"
        self.ema_days = 60
        self.short_days = 22
        self.long_days = 60
        self.adjust_window_days = 10
        
        '''預設權重為等權重，TLT預設為不配置'''
        ticker_list = self.universe_data_df_dict["adjclose"].columns.to_list()
        target_weight_dict = dict()
        target_vol_dict = dict()

        #依照2008年左右S&P500各部門的權重配置
        # target_weight_dict = {
        #     "XLE":0.1, 
        #     "XLP":0.1, 
        #     "XLI":0.1, 
        #     "XLY":0.15, 
        #     "XLV":0.1, 
        #     "XLF":0.15,
        #     "XLK":0.2, 
        #     "XLU":0.05, 
        #     "XLB":0.05, 
        #     "TLT":0
        # }

        # target_weight_dict = {
        #     "NFLX":1,
        # }
        
        for ticker in ticker_list:
            if ticker != "TLT":
                target_weight_dict[ticker] = 1/(len(ticker_list)-1)

        self.original_weight_series = pd.Series(target_weight_dict)
        
    def cal_weight(self):
        changeDate_list = self._get_change_tradeDate_list(self.freq)
        price_df = self.universe_data_df_dict["adjclose"].shift(1)
        
        #去掉TLT的價格資料
        price_df.drop(columns=["TLT"], inplace=True)
        price_df.dropna(axis=0, inplace=True)
        
        #Universe的標的列表（已去除TLT）
        ticker_list = price_df.columns.to_list()

        #計算每日百分比變動
        percent_df = price_df.pct_change()
        
        #過去一年的年化波動df
        rolling_yr1_vol = percent_df.rolling(252, min_periods=1).std() * sqrt(252)
        percentile_80_vol_df = rolling_yr1_vol.rolling(252, min_periods=1).quantile(0.8)
        
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
            
            # print("\nDate:\n", date)
            # recent_bb_df = bull_bear_df[bull_bear_df.index <= date]
            # print("bull_bear_df:\n", bull_bear_df[bull_bear_df.index <= date].tail(5))
            # print("bull_count_series:\n", bull_count_series)
            # print("bear_count_series:\n", bear_count_series)
            # print("original_target_vol_series:\n", original_target_vol_series)
            # print("\n")
            
            # print("bull_target_vol_series:\n", bull_target_vol_series)
            # print("bear_target_vol_series:\n", bear_target_vol_series)
            # print("recent_bull_vol_series:\n", recent_max_vol_df.loc[date,bull_ticker_list])
            # print("recent_bear_vol_series:\n", recent_max_vol_df.loc[date,bear_ticker_list])
            # print("bull_raw_target_weight_series:\n", bull_raw_target_weight_series)
            # print("bear_raw_target_weight_series:\n", bear_raw_target_weight_series)
            # print("bull_target_weight_series\n", bull_target_weight_series)
            # print("bear_target_weight_series\n", bear_target_weight_series)
        
        weight_df["TLT"] = 1-weight_df.sum(axis=1)
        return weight_df

