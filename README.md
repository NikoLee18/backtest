# Parallel Backtest Package

I wrote a framework using python in 2020 to do backtest for quantitative investment strategies. However, it turned out, 
in practice, that my first framework was not efficient. Even a simple strategy based on Fama-French factors would take 
up to 1 hr to backtest.

Therein lies myriad of problems. First, it implements strategies trade-day by trade-day, so GIL of python greatly 
confines how we can boost this process. Next, I attempted to finish calculation of some basic indicators of account 
(and strategy) as the framework working out investment decisions, so that we can add stopping-loss to it. But this try 
implies that we are doing some heavy numerical work serially, which should have been done in a vectorized method. 

The biggest problem is the prive database. By the time I wrote 1st edition of backtest code, I had no knowledge of 
high-performance databases like MongoDB so that version exploits a pandas.DataFrame object to fulfill CRUD functions of 
a database. 

In this 2nd edition, I fixed all the problems mentioned. And followings are some brief introduction.


## Quick Start
If you are not interested in project structure, you may just write a strategy like:

    def run_one(run_dt: list, save_path: str, db_host='LocalHost', db_port=27017):
    
        pos = {}
        re = {}
        num = 0
    
        for d in run_dt:
            num += 1
            if num % 2 == 0:
                pos[d] = ParaPosition(d, host=db_host, port=db_port)
    
                pos[d].target_value(['000001.XSHE'], {'000001.XSHE': 1000})
                re[d] = pos[d].get_account_info()
                pos[d].disconnect()
            else:
                pos[d] = ParaPosition(d, host=db_host, port=db_port)
    
                pos[d].target_value(['002603.XSHE', '600000.XSHG'], {'002603.XSHE': 1000, '600000.XSHG':2000})
                re[d] = pos[d].get_account_info()
                pos[d].disconnect()
    
        output_results(re, save_path)

and input this function handle to function: backtest_wrapper , and specify start_date and end_date of that function,
input your username and password for package jqdatasdk. Run your script and code will automatically backtest your 
strategy and output results in directory: resulting which is named as your backtest datetime.

## Project Structure
You may read my code on your own.

Maybe I will upload a video on my 
[WeChat Official Account: Oliver the Cat](https://mp.weixin.qq.com/s?__biz=Mzg4MDM1OTY4NQ==&mid=2247484464&idx=1&sn=9a70506995bdf0291bba80c906f01be6&chksm=cf77245af800ad4c193be4c0c55edb2942f730dd0641db6a6574b6cc914a050a73b7aa825945&token=1450836220&lang=zh_CN#rd)
, or my [Zhihu.com Account: Li Changhao](https://www.zhihu.com/people/li-chang-hao-29-22), or [bilibili.tv channel: nikolee18](https://space.bilibili.com/5422938), 
[YouTube.com channel:NikoLee1118](https://www.youtube.com/channel/UCoL5FUPpVvvP0qIavllff7Q), 
