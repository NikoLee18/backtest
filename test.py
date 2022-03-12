import pymongo
from PriceSector import *
import pandas as pd


if __name__ == '__main__':
    start = '2020-12-01'
    end = '2021-02-04'
    secs = ['000001.XSHE', '000002.XSHE', '600000.XSHE']
    pr = MinutePrice()
    df = pr.get_minute(secs=secs, start_date=start, end_date=end, fields=None)
    print(df.head(30))