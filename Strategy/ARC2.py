from self_finance_database_tool import *
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from math import sqrt
import sys
from .strategy_interface import *

class ARC_advance(Strategy):
    def reset(self):
        self.strategy_name = "ARC非定期調倉版"
        '''預設權重為等權重，TLT預設為不配置'''
        ticker_list = self.universe_data_df_dict["adjclose"].columns.to_list()
        self.default_weight_dict = dict()
        target_vol_dict = dict()
        
        for ticker in ticker_list:
            if ticker != "TLT":
                self.default_weight_dict[ticker] = 1/(len(ticker_list)-1)
    
    #讀入外部設定的超參數
    def parameters_set(self):
        self.price_ema_days = self.hyperParameters_dict["price_ema_days"]
        self.vol_days = self.hyperParameters_dict["vol_days"]
        self.rolling_window_days = self.hyperParameters_dict["rolling_window_days"]
        self.determine_ratio = self.hyperParameters_dict["determine_ratio"]
        self.adjust_speed = self.hyperParameters_dict["adjust_speed"]

    def dataset_prepare(self, price_df):
        data_set = dict()

        #去除TLT的價格資料
        price_df.drop(columns=["TLT"], inplace=True)
        price_df.dropna(axis=0, inplace=True)

        #Universe的標的列表（已去除TLT）
        ticker_list = price_df.columns.to_list()

        #每日百分比變動的df
        percent_df = price_df.pct_change()
        # percent_df[percent_df>0] = 0

        #股價EMA均線的df
        price_ema_df = price_df.ewm(span=self.price_ema_days).mean()
        price_ema_df.dropna(axis=0, inplace=True)

        #年化波動度EMA均線的df
        recent_vol_df = percent_df.rolling(self.vol_days, min_periods=1).std() * sqrt(252)
        recent_vol_df.dropna(axis=0, inplace=True)

        #以過去一年的年化波動的df，作為歷史波動度
        rolling_yr1_vol_df = percent_df.rolling(252, min_periods=1).std() * sqrt(252)
        rolling_yr1_vol_df.dropna(axis=0, inplace=True)
        historical_vol_df = rolling_yr1_vol_df
        
        #多頭條件1: 近期波動度在歷史波動度之下
        raw_bull_bear_df1 = historical_vol_df > recent_vol_df
        #多頭條件2: 昨日收盤價在EMA均線之上
        raw_bull_bear_df2 = price_df > price_ema_df
        #取兩條件的交集或聯集，得到初步的強弱勢判斷
        raw_bull_bear_df = raw_bull_bear_df1 | raw_bull_bear_df2
        # raw_bull_bear_df = raw_bull_bear_df1
        #以初步的強弱勢判斷在過去一段時間中，強弱勢總天數是否超過決定比率來得到正式的強弱勢判斷
        bull_bear_df = raw_bull_bear_df.rolling(self.rolling_window_days, min_periods=1).sum() >= (self.rolling_window_days * self.determine_ratio)
        bull_bear_df = bull_bear_df[(bull_bear_df.index >= self.start_date) & (bull_bear_df.index <= self.end_date)]
        #將強勢標註為1，弱勢標註為-1
        bull_bear_df = bull_bear_df.astype(int)
        bull_bear_df[bull_bear_df==False] = -1
        
        data_set["price_df"] = price_df
        data_set["ticker_list"] = ticker_list
        data_set["percent_df"] = percent_df
        data_set["recent_vol_df"] = recent_vol_df
        data_set["bull_bear_df"] = bull_bear_df
        data_set["historical_vol_df"] = historical_vol_df
        return data_set

    def cal_weight(self):
        #讀入超參數
        self.parameters_set()
        
        #去掉TLT的價格資料
        price_df = self.universe_data_df_dict["adjclose"].shift(1)
        data_set = self.dataset_prepare(price_df)
        tradeDate_list = self._get_backtest_period_tradeDate_list()
        
        ticker_list = data_set["ticker_list"]
        bull_bear_df = data_set["bull_bear_df"]
        
        tradeDate_list = self._get_backtest_period_tradeDate_list()
        raw_signal_df = pd.DataFrame(index=tradeDate_list, columns=ticker_list)
        signal_df = pd.DataFrame(index=tradeDate_list, columns=ticker_list)
        
        for index, ticker in enumerate(ticker_list, 1):
            percentage = 100*index/len(ticker_list)
            sys.stdout.write('\r'+"權重計算：訊號計算完成度{percentage:.2f}%".format(percentage=percentage))
            changeDate_list = [self.start_date, self.end_date]
            for i in range(1, len(bull_bear_df)):
                if bull_bear_df.iloc[i].loc[ticker] != bull_bear_df.iloc[i-1].loc[ticker]:
                    changeDate_list.extend(bull_bear_df.index[i:i+self.rolling_window_days])
            
            #去除重複日期
            changeDate_list = list(set(changeDate_list))
            changeDate_list.sort()
            raw_signal_df.loc[:,ticker] = bull_bear_df.loc[changeDate_list, ticker]
            
        raw_signal_df.dropna(how='all', inplace=True)

        for index, ticker in enumerate(ticker_list, 1):
            percentage = 100*index/len(ticker_list)
            sys.stdout.write('\r'+"權重計算：權重計算完成度{percentage:.2f}%".format(percentage=percentage))
            cur_index=0
            con_sigal_df_list = []
            con_sigal_df = pd.DataFrame(index=tradeDate_list, columns=ticker_list)

            for i in range(1, len(raw_signal_df)):
                if (raw_signal_df.iloc[i].loc[ticker] != raw_signal_df.iloc[i-1].loc[ticker]) or (i==len(raw_signal_df)-1):
                    #把最後一天也納入權重計算（不論最後一天是否有變號）
                    if i==len(raw_signal_df)-1:
                        i+=1
                    con_sigal_df = (raw_signal_df.iloc[cur_index:i].loc[:,ticker]).rolling(self.rolling_window_days, min_periods=1).sum()
                    cur_index = i
                    con_sigal_df_list.append(con_sigal_df)
                
            signal_df.loc[:,ticker] = pd.concat(con_sigal_df_list)
        
        signal_df.dropna(how='all', inplace=True)
        
        target_vol_df = data_set["historical_vol_df"].loc[signal_df.index,:] * (1+self.adjust_speed*signal_df/self.rolling_window_days)
        target_vol_df[target_vol_df<0] = 0
        
        raw_weight_df = (target_vol_df / data_set["recent_vol_df"].loc[signal_df.index,:]) * pd.Series(self.default_weight_dict)
        raw_weight_df.ffill(inplace=True)
        
        total_weight_series = raw_weight_df.sum(axis=1)
        total_weight_series[total_weight_series<1] = 1
        raw_weight_df = raw_weight_df.div(total_weight_series, axis=0)
        
        # raw_weight_df["TLT"] = 1-raw_weight_df.sum(axis=1)
        
        # folderPath = "Temp_Data"
        # Performance_fileName = os.path.join(folderPath, "Performance.xlsx")
        # Performance_excel = pd.ExcelWriter(Performance_fileName,engine='xlsxwriter')   # Creating Excel Writer Object from Pandas  
        
        # #儲存成Excel檔
        # (raw_signal_df).to_excel(Performance_excel, sheet_name='raw_signal_df')
        # (signal_df).to_excel(Performance_excel, sheet_name='signal_df')
        # (raw_weight_df).to_excel(Performance_excel, sheet_name='raw_weight_df')
        # (bull_bear_df).to_excel(Performance_excel, sheet_name='bull_bear_df')
        # Performance_excel.save()
        print(raw_weight_df)
        return raw_weight_df
