from itertools import product
from main import *

#產生試驗參數字典
def parameter_dict_generate(p_dict, p_test_name_list, low_bound, high_bound, times):
    p_range_dict = p_dict.copy()
    for p in p_test_name_list:
        p_list = p_dict[p]*(1+np.linspace(low_bound, high_bound, times))   
        #若原參數為整數，須將調整後的試驗參數也四捨五入為整數
        #以避免部分參數不得使用小數   
        if isinstance(p_dict[p], int):
            for i in range(len(p_list)):
                p_list[i] = int(round(p_list[i]))
                p_list = p_list.astype(int)
        p_range_dict[p] = p_list
    
    #將各項參數調整times次後，得到times個試驗參數，再取笛卡兒積，得到完整的試驗參數字典
    return p_range_dict, list(dict(zip(p_dict.keys(), values)) for values in product(*p_range_dict.values()))

#找出Sharpe最大的一組參數與其Sharpe值，並回傳試驗參數字典的試驗結果
def parameter_opmitize(strategy_kind, universe_ticker_list, options, parameters_dict_list, low_bound = -1, high_bound=1, times=5):
    max_sharpe = 0
    argmax_dict = dict()
    for index, parameters_dict in enumerate(parameters_dict_list, 1):
        percentage = 100*index/len(parameters_dict_list)
        sys.stdout.write('\r'+"敏感度測試：完成度{percentage:.2f}%\n".format(percentage=percentage))
        #逐次替換超參數字典
        options["hyperParameters_dict"] = parameters_dict
        backtest = Backtest(ARC_advance, universe_ticker_list, options)
        backtest.activate()
        backtest.evaluate(show=False)
        print("Parameters:", parameters_dict)
        print("Sharpe:", backtest.performance["Average Sharpe"])
        print("\n\n")
        #紀錄每組試驗參數的績效表現
        parameters_dict["Average Sharpe"] = backtest.performance["Average Sharpe"]

        #對比績效是否超過前高
        if backtest.performance["Average Sharpe"] > max_sharpe:
            max_sharpe = backtest.performance["Average Sharpe"]
            argmax_dict = parameters_dict

    return max_sharpe, argmax_dict, parameters_dict_list

def sensitivity_analysis(p_range_dict, p_dict_list):
    import statistics

    for p_name, p_range_list in p_range_dict.items():
        if p_name not in p_test_name_list:
            continue

        median_sharpe_list = list()
        for p_value in p_range_list:
            sharpe_list = list()
            for outcome in p_dict_list: 
                if outcome[p_name] == p_value:
                    sharpe_list.append(outcome["Average Sharpe"])
            
            median_sharpe_list.append(statistics.median(sharpe_list))
        
        print("參數:", p_name)
        print("範圍:", p_range_list)
        print("夏普:", median_sharpe_list)
        a = pd.Series(median_sharpe_list, index=p_range_list)
        a.plot(title="Sensitivity Analysis", grid=True) 
        plt.xlabel(p_name)
        plt.ylabel('Sharpe')
        plt.show()
        print(a)        

ARC_single_ticker_list = ["SPY", "TLT"]
p_test_name_list = ["rolling_window_days", "determine_ratio", "adjust_speed"]

hyperParameters_dict = {
            "price_ema_days":[60],
            "vol_days":[22],
            "rolling_window_days":10,
            "determine_ratio":0.5,
            "adjust_speed":2.0,
        }

options = { "start_date" : "2010-01-01",
            "end_date" : "2021-03-01",
            "commission_rate":0.004,
            "initial_value":1,
            "freq":"month",
            "decay_days": 0,
            "short_permit":False,
            "backtest_name":'',
            "universe_name":'ARC_single_ticker_list',
            "country":"US",
            "benchmark":"SPY",
            "hyperParameters_dict":hyperParameters_dict,
            "database_path":"/Users/yahoo168/Documents/programming/Quant/Database",
            }

universe_ticker_list = ["SPY", "TLT"]

low_bound = -0.9
high_bound = 1
times = 8
p_range_dict, parameters_dict_list = parameter_dict_generate(options["hyperParameters_dict"], p_test_name_list, low_bound=low_bound, high_bound=high_bound, times=times)
max_sharpe, argmax_dict, parameters_dict_list = parameter_opmitize(ARC_advance, universe_ticker_list, options, parameters_dict_list, low_bound=low_bound, times=times)
print(max_sharpe, argmax_dict)
print()
sensitivity_analysis(p_range_dict, parameters_dict_list)

