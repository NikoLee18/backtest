import pandas as pd
from para_backtest import ParaPosition
import multiprocessing as mp
import jqdatasdk as jq
import os
import datetime
from parallel_toolkit import task_distributor


if __name__ == "__main__":
    from run_backtest import backtest_wrapper, run_one
    # get daily holding
    backtest_wrapper(run_one, start_date='2021-01-01', end_date='2021-10-01',
                     username='18006001474', password='LeeChanghao18', db_host='192.168.1.103', db_port=27017)



