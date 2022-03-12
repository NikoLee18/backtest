import pandas as pd
from para_backtest import ParaPosition
import multiprocessing as mp
import jqdatasdk as jq
import os
import datetime
from parallel_toolkit import task_distributor


def run_one(run_dt: list, save_path: str):
    pos = {}
    re = {}
    num = 0
    for d in run_dt:
        num += 1
        if num % 2 == 0:
            pos[d] = ParaPosition(d)

            pos[d].target_value(['000001.XSHE'], {'000001.XSHE': 1000})
            re[d] = pos[d].get_account_info()
            pos[d].disconnect()
        else:
            pos[d] = ParaPosition(d)

            pos[d].target_value(['002603.XSHE'], {'002603.XSHE': 1000})
            re[d] = pos[d].get_account_info()
            pos[d].disconnect()

    df = pd.concat(re.values())
    df.to_excel(save_path)


def multi_core_strategy(save_path):
    NUM_PROCESS = 4
    tradedays = jq.get_trade_days(start_date="2021-01-01",end_date="2021-10-01")
    tradedays = [i.strftime("%Y-%m-%d") for i in tradedays]
    new_td = []
    for i in range(0, len(tradedays), 2):
        new_td.append(tradedays[i])
    tradedays = task_distributor(NUM_PROCESS, new_td)
    processes = []
    for d in tradedays:
        print(d[0]+"开始进程")
        p = mp.Process(target=run_one, args=(d, save_path+'/'+d[0]+'.xlsx'))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()

    print("进程全部结束")


if __name__ == "__main__":
    jq.auth('18006001474', 'LeeChanghao18')
    path = "testing"
    try:
        os.mkdir(path)
    except FileExistsError:
        path = path + datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        os.mkdir(path)
    print("auth finished\n")
    multi_core_strategy(path)
    print(2)
    jq.logout()



