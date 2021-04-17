from self_finance_database_tool import *
from utils import *
from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import json
import pickle
from Strategy import *

class Backtest(object):
    """
    Backtest為一回測物件，必要的輸入值為策略類別與設定回測選項（options）
    
    物件方法概覽：
        & activate: 呼叫策略以取得權重df，並計算此權重在過去的每日價值變動
        & evaluate: 依照計算完成的每日百分比變動計算績效指標
        & save_record: 將各績效指標以及策略參數存檔
    """
    def __init__(self, strategyClass, universe_ticker_list, options):
        self.options = options
        
        #透過路徑建立資料庫物件，以便取用資料
        self.db = Database(self.options["database_path"])
        self.universe_ticker_list = universe_ticker_list
        
        #讀入策略類別（策略物件尚未建立）
        self.strategyClass = strategyClass
        
        #調整原始設定的日期，改為最接近該日的下一個交易日（若該日即為交易日則不變）
        self.start_date = self.db.get_next_tradeDate(datetime.strptime(self.options["start_date"], "%Y-%m-%d"), country=self.options["country"])
        self.end_date = self.db.get_next_tradeDate(datetime.strptime(self.options["end_date"], "%Y-%m-%d"), country=self.options["country"])
        
        #存放Universe的各類資料，在回測過程中計算使用
        #以及傳給策略物件計算權重，避免資料重複讀入。
        self.universe_data_df_dict = dict()

        #存放投資組合績效的各類資料，便於績效比對。
        self.performance = dict()
        
        #存放Benchmark的各類資料，便於績效比對。
        self.benchmark_data_dict = dict()
        
        #紀錄每次換倉的手續費序列
        self.performance["commission_fee_series"] = pd.Series()

    #待改：目前用到的資料不多
    def _load_universe_data(self, ticker_list):
        # self.universe_data_df_dict["open"] = self.db.get_universe_df(self.universe_ticker_list, data_type="open", end_date=self.end_date, data_format="all")
        # self.universe_data_df_dict["high"] = self.db.get_universe_df(self.universe_ticker_list, data_type="high", end_date=self.end_date, data_format="all")
        # self.universe_data_df_dict["low"] = self.db.get_universe_df(self.universe_ticker_list, data_type="low", end_date=self.end_date, data_format="all")
        self.universe_data_df_dict["close"] = self.db.get_universe_df(self.universe_ticker_list, data_type="close", end_date=self.end_date, data_format="all")
        self.universe_data_df_dict["adjclose"] = self.db.get_universe_df(self.universe_ticker_list, data_type="adjclose", end_date=self.end_date, data_format="all")
        # self.universe_data_df_dict["volume"] = self.db.get_universe_df(self.universe_ticker_list, data_type="volume", end_date=self.end_date, data_format="all")

    #建立Strategy物件，並進行參數配置
    def _strategy_preprocess(self):
        self.strategy = self.strategyClass(self.start_date, self.end_date, self.universe_data_df_dict, self.options)
        self.strategy.reset()
    
    #呼叫Strategy物件以取得權重矩陣，並依照Decay設置調節交易日
    #Strategy會回傳調倉日的日期與當日的權重，格式為df
    def _get_strategy_weight_df(self):
        decay_days = self.options["decay_days"]
        self.weight_df = self.strategy.cal_weight()
        if decay_days != 0:
            if decay_days < 0 :
                print("Warn::You're forward looking.")
            
            #先加/減原始交易日後調整到下一個真實存在的交易日
            original_date_series = (self.weight_df).index
            next_date_series = original_date_series + timedelta(days=decay_days)
            self.weight_df.index = pd.Index(map(self.db.get_next_tradeDate, next_date_series))

    # 以調倉日拆分區間，以計算報酬
    def _cal_return(self):
        changeDate_list = self.weight_df.index
        holder_ticker_list = self.weight_df.columns

        #取得持有標的之調整價（可能有些標的沒有被選到）
        self.adjclose_df = self.universe_data_df_dict["adjclose"].loc[:,holder_ticker_list]
        
        #以兩調倉日作為區間起始
        cal_interval=[]
        for i in range(1, len(changeDate_list)):
            cal_interval.append((changeDate_list[i-1], changeDate_list[i]))
        
        #透過_cal_interval_return計算各區間績效，再全部合併
        interval_sum_percent_series_list = list()
        interval_commission_fee_list = list()
        print()       
        for index, interval in enumerate(cal_interval, 1):
            percentage = 100*index/len(cal_interval)
            sys.stdout.write('\r'+"回測計算：完成度{percentage:.2f}%".format(percentage=percentage))
            interval_sum_percent_series, interval_commission_fee = self._cal_interval_return(interval)
            interval_sum_percent_series_list.append(interval_sum_percent_series)
            interval_commission_fee_list.append(interval_commission_fee)
        print()

        #合成各換倉區間回測所得的百分比變動序列
        sum_percent_series = pd.concat(interval_sum_percent_series_list)
        #透過百分比變動計算資產組合的日報酬
        sum_value_series = (sum_percent_series+1).cumprod() * self.options["initial_value"]
        self.sum_percent_series = sum_percent_series
        self.sum_value_series = sum_value_series
        self.performance["Commission Fee Series"] = pd.Series(interval_commission_fee_list, index=changeDate_list[1:])
        
    # 回測起始日期與結束日期最近的後一個交易日，也應在調倉日中
    # 計算換倉區間的績效變動，換倉日前一日收盤後換倉，隔日為新倉位。
    # 回傳該區間的每日資產價值波動以及奇摩換倉日的手續費
    def _cal_interval_return(self, interval):
        s_date = interval[0]
        e_date = interval[1]
        
        #將起始日期各往前調一個交易日，以實現換倉日當天開盤前即換完倉
        s_date_last_tradeDate = self.db.get_last_tradeDate(s_date-timedelta(days=1), country=self.options["country"])
        e_date_last_tradeDate = self.db.get_last_tradeDate(e_date-timedelta(days=1), country=self.options["country"])
        
        #取得期初（回測開始日 或換倉日）的權重
        s_weight = self.weight_df.loc[s_date,:]
        
        #取得下一次換倉日的權重，以便計算調倉手續費
        next_weight = self.weight_df.loc[e_date,:]
        
        #待改：確認權重和沒有問題（不應該放在這裡）
        #精度計算問題可能導致權重和為1的實際權重比1多一點點
        assert(s_weight.sum() <= 1.001)
        cash_weight = 1 - s_weight.sum()
        
        #截取該區間的價格df（往起始日前多取一天，以取得起始日當日的百分比變動）
        price_df = self.adjclose_df[self.adjclose_df.index >= s_date_last_tradeDate]
        price_df = price_df[price_df.index <= e_date_last_tradeDate]
        percent_df = price_df.pct_change()
        holder_ticker_list = price_df.columns
        
        #將起始日的前一日變動設為0，以避免起始日的百分比變動出錯
        percent_df.fillna(0, inplace=True)
        
        #將各標的取連乘，並乘上起始日的權重分配，#計算本次換倉區間的加權累積報酬
        weighted_cumprod_df = (percent_df+1).cumprod() * s_weight
        sum_cumprod_series = weighted_cumprod_df.sum(axis=1) + cash_weight
        sum_percent_series = sum_cumprod_series.pct_change()

        #去除換倉日前一交易日的資料（原是為配合計算百分比變動而加入）
        sum_percent_series = sum_percent_series[1:]

        # 待改：考量放空的情況
        # neg_comprod_df = (-percent_df[neg_weight.index]+1).cumprod()        
        commission_fee = self._cal_commision_fee(s_weight, cash_weight, weighted_cumprod_df.iloc[-1,:], next_weight)
        sum_percent_series[-1] -= commission_fee
        
        return sum_percent_series, commission_fee

    #計算交易成本（手續費)
    def _cal_commision_fee(self, s_weight, cash_weight, e_value, next_weight):
        #計算因本換倉期間價格變動，導致的權重偏移
        commission_fee = 0
        if e_value.sum() != 0:
            e_weight = (e_value / (e_value.sum()+cash_weight))
            #以待調整權重佔總部位比例計算手續費
            #因原始手續費（0.004)為買進賣出的總數，單方向交易僅一半
            commission_series = abs(e_weight - next_weight) * (self.options["commission_rate"])/2
            commission_fee = commission_series.sum()
        return commission_fee

    def _load_benchmark_data(self, benchmark):
        # Case1: Benchmark為str，代表為某實體標的(e.g: "SPY")
        if isinstance(benchmark, str):
            benchmark_adjclose = self.db.get_universe_df([benchmark], data_type="adjclose", data_format="all")
            adjclose_percent_series = benchmark_adjclose.squeeze().pct_change().fillna(0)
            #對照資產組合的時間序列，抓取對應的benchmark序列
            self.benchmark_data_dict["percent"] = adjclose_percent_series[self.sum_percent_series.index]
            self.benchmark_data_dict["value"] = (1+self.benchmark_data_dict["percent"]).cumprod() * self.options["initial_value"]
            
        #待改：若benchmark為Backtest類（其他策略）
        else:
            pass
    pass

    #依序啟動回測過程中各個函數，完成回測績效計算
    def activate(self):
        start_time = time.time()
        self._load_universe_data(self.universe_ticker_list)
        self._strategy_preprocess()
        self._get_strategy_weight_df()
        self._cal_return()
        self._load_benchmark_data(self.options["benchmark"])
        end_time = time.time()
        print("回測共耗費{:.3f}秒\n".format(end_time - start_time))

    # def parameter_opmitize(self, times=0, percent_tick=0):
    #     start_time = time.time()
    #     self._load_universe_data(self.universe_ticker_list)
        
    #     for i in range(times):
    #         s
    #     self._strategy_preprocess()
    #     self._get_strategy_weight_df()
    #     self._cal_return()
    #     self._load_benchmark_data(self.options["benchmark"])
    #     pass

    def show_figure(self):
        self.sum_value_series.plot(label="Algorithm", legend=True)
        self.benchmark_data_dict["value"].plot(label="Benchmark", legend=True)
        plt.show()

    #評估策略表現
    def evaluate(self, show=True):
        avg_fee = cal_avg_commision(self.performance["Commission Fee Series"], freq=self.options["freq"])
        avg_return, avg_volatility, avg_sharpe = cal_basic_performance(self.sum_percent_series)
        MDD, date_interval = cal_maxdrawdown(self.sum_value_series)
        # de_extreme_ratio_df, ratio_df = cal_profit_ratio(self.percent_df)
        win_rate = cal_win_rate(self.sum_percent_series, self.benchmark_data_dict["percent"])
        performance_per_Yr_df = cal_performance_per_yr(self.sum_percent_series, self.benchmark_data_dict["percent"])
        performance_per_Qr_df = cal_performance_per_quarter(self.sum_percent_series, self.benchmark_data_dict["percent"])

        self.performance["Average Annualized Return"] = avg_return
        self.performance["Average Annualized Volatility"] = avg_volatility
        self.performance["Average Sharpe"] = avg_sharpe
        self.performance["MDD"] = MDD
        self.performance["MDD INRERVAL"] = date_interval
        self.performance["WIN RATE"] = win_rate
        self.performance["Annualized Avg Commision Fee"] = avg_fee

        # self.performance["De_extreme Ratio"] = de_extreme_ratio_df
        # self.performance["Ratio"] = ratio_df

        self.performance["Performance Per Yr"] = performance_per_Yr_df
        self.performance["Performance Per Qr"] = performance_per_Qr_df

        if show == True:
            print("Annualized Avg Commision Fee:", round(100*avg_fee, 2),'%')
            print("Rolling Return:", avg_return)
            print("Rolling Volatility:", avg_volatility)
            print("Rolling Sharpe:", avg_sharpe)
            print("MDD Interval:", datetime.strftime(date_interval[0], "%Y-%m-%d"),'~',
                                   datetime.strftime(date_interval[1], "%Y-%m-%d"))
            print("MDD:", MDD)
            print("WIN RATE:", win_rate)
            print("\nPerformance Per Year:\n", performance_per_Yr_df)
            print("\nPerformance Per Quarter:\n", performance_per_Qr_df)
            print("\nPortfolio Value:\n", self.sum_value_series)
            
    #將回測紀錄存於以策略命名的資料夾中，資料夾結構（策略/Universe/日期）
    #超參數另以txt檔寫入，預設為不儲存
    def save_record(self, save_log=False): 
        #建立資料夾名稱
        strategy_filePath = os.path.join("Record", self.strategy.strategy_name)
        universe_filePath = os.path.join(strategy_filePath,  self.options["universe_name"])
        date_filePath = os.path.join(universe_filePath, self.options["start_date"]+'_'+self.options["end_date"])
        folderPath = date_filePath
        makeFolder(folderPath)

        Performance_fileName = os.path.join(folderPath, "Performance.xlsx")
        Performance_excel = pd.ExcelWriter(Performance_fileName,engine='xlsxwriter')   # Creating Excel Writer Object from Pandas  
        
        #儲存成Excel檔
        (self.sum_value_series).to_excel(Performance_excel, sheet_name='Sum_Value')
        (self.weight_df).to_excel(Performance_excel, sheet_name='Weight')
        (self.performance["Performance Per Yr"]).to_excel(Performance_excel, sheet_name='Per Yr')
        (self.performance["Performance Per Qr"]).to_excel(Performance_excel, sheet_name='Per Qr')
        Performance_excel.save()

        perfomance_item_for_log_list = ["Average Annualized Return", 
        "Average Annualized Volatility", 
        "Average Sharpe"]
        
        if save_log:
            with open(logs_filePath, 'w') as file:
                file.write("Documents:\n\n")
                file.write(docs+"\n")
                file.write('_'*20 +'\n')
                file.write("Performance:\n\n")
                
                if self.performance:
                    for item in perfomance_item_for_log_list:
                        file.write(item+':')
                        file.write(json.dumps(self.performance[item])+"\n")

                else:
                    file.write("{ Not Yet Evaluate Performance. }\n")

                info_dict = self.strategy.get_infomation_parameters_dict()
                docs = self.strategy.__doc__
                logs_filePath = os.path.join(folderPath,'logs.txt')
                file.write('_'*20 +'\n')
                file.write("Parameters:\n\n")
                file.write(json.dumps(info_dict))

#0050的ticker列表(以2020/10/23的資料為基準)，其中'2474_TW'可成, '2633_TW'台灣高鐵, '2327_TW'國巨, 因資料缺失而移除。
db = Database("/Users/yahoo168/Documents/programming/Quant/Database")

TW50_tickers_list = db.get_ticker_list("TW50")
SP500_ticker_list = db.get_ticker_list("SP500")

broad_index_list = ["SPY","QQQ","EMB","LQD","HYG","TLT","GLD","USO","FRI"]
SPX_sector_ticker_list = ["XLE", "XLP", "XLI", "XLY", "XLV", "XLF","XLK", "XLU", "XLB", "TLT"]
# Equal_weight_benchmark = Backtest(Equal_weight, universe_ticker_list, options)

if __name__ == '__main__':
    
    hyperParameters_dict = {
            "price_ema_days":60,
            "vol_days":10,
            "rolling_window_days": 10,
            "determine_ratio":0.3,
            "adjust_speed":2,
        }

    ARC_single_ticker_list = ["SPY", "TLT"]

    options = { "start_date" : "2010-01-01",
            "end_date" : "2021-03-01",
            "commission_rate":0.004,
            "initial_value":1,
            "freq":"month",
            "decay_days": 0,
            "short_permit":False,
            "backtest_name":'',
            "universe_name":'SPY',
            "country":"US",
            "benchmark":"SPY",
            "hyperParameters_dict":hyperParameters_dict,
            "database_path":"/Users/yahoo168/Documents/programming/Quant/Database",
            }

    universe_ticker_list = ARC_single_ticker_list
    # backtest = Backtest(ARC_advance, universe_ticker_list, options)
    backtest = Backtest(Equal_weight, ["SPY"], options)

    backtest.activate()
    backtest.evaluate()
    backtest.save_record()
    backtest.show_figure()

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