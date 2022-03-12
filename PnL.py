import pymongo
import pandas as pd
import numpy as np
import os
from para_backtest import NormalClass
from PriceSector import PriceSector


class PnL:

    def __init__(self, path: str, start_date: str, end_date: str, host='LocalHost', port=27017,
                 normal_scale='thousand'):
        """

        :param path: 储存回测结果的 >>相对路径<<
        :param start_date:
        :param end_date:
        :param host: default 'LocalHost' 不用动
        :param port: default 27017 不用动
        :param normal_scale: default 'thousand' 也就是设初始账户有1000元。 不用动
        """
        # March13修改 改用PriceSector取数据
        self.pr = PriceSector()
        # 创建好与mongodb的连接。  我们归因只用到日频
        self.client = pymongo.MongoClient(host=host, port=port)
        db = self.client.get_database('daily')
        self.db = db

        # 取出交易日日历，把没有交易的日子补上
        col0 = db.get_collection('tradedays')
        col0 = col0.find({'date': {
            "$gte": pd.to_datetime(start_date),
            "$lte": pd.to_datetime(end_date)
        }})
        self.tradedays = [i['date'] for i in col0]

        # 先把原始的pnl数据读取出来
        re = []
        files = os.listdir(path)
        for f in files:
            re.append(pd.read_excel(path + '/' + f, index_col=0))
        raw = pd.concat(re)
        # 防止出错，先sort一下
        raw.sort_index(axis=0, inplace=True)
        # 记录下调仓日
        self.adj_days = raw.index

        # 2022Mar13改动：在并行回测部分，为了保证数值精度可以乘1000，所以这里要进行类似的scaling
        assert normal_scale in NormalClass.__dict__
        self.normalization = NormalClass.__dict__[normal_scale]

        # 调整原始数据的形式
        column = raw.columns.tolist()
        column.remove('pos')
        column.append('pos')  # 防止表示仓位的pos列乱跑
        self.hold = raw.loc[:, column]
        self.hold.columns = column[:-1] + ['cash']
        self.hold.loc[:, 'cash'] = self.normalization * (1 - self.hold.loc[:, 'cash'])  # 现金也是按照比例表示

        # 现在开始调整self.hold的形式 把空缺的填上：用前面的值填充，所以ffill
        self.hold.fillna(value=0, inplace=True)
        self.hold = pd.DataFrame(data=self.hold, index=self.tradedays)
        self.hold.fillna(method='ffill', inplace=True)

        # 下面的generate_portfolio_ret需要给对象填充一个portfolio_value,这里先初始化
        self.portfolio_value = None

    def generate_portfolio_ret(self, method=None):
        """
        从并行回测的输出文件夹读取数据，补全数据，并且根据pos（仓位数据）计算手上的现金
        :param method: 暂未使用
        :return: pd.DataFrame
                000001.XSHE	002603.XSHE	cash
        2021-01-04	0.000000	1.439444	0.0
        2021-01-05	0.000000	1.472222	0.0
        2021-01-06	0.000000	1.619444	0.0
        2021-01-07	0.000000	1.586667	0.0
        2021-01-08	1.595508	0.000000	0.0
        """
        # # 从MongoDB取出数据
        # col = self.db.get_collection('daily')
        # data = col.find({
        #     'code': {'$in': self.hold.columns.tolist()[:-1]},
        #     'date': {'$gte': self.tradedays[0], '$lte': self.tradedays[-1]}  # 最后一列是持现金比例
        # }, projection={'_id': 0, 'code': 1, 'date': 1, 'close': 1})
        # data = pd.DataFrame(list(data))
        data = self.pr.get_price(secs=self.hold.columns.tolist()[:-1], start_date=self.hold.index[0],
                                 end_date=self.hold.index[-1], fields=['date', 'code', 'close'])  # [:-1]因为最后一列记录cash

        # 整理成需要的dataframe样子
        data = self._reformater_(data)
        data.sort_index(axis=0, inplace=True, ascending=True)  # 一定要ascending，日期早的往前放
        df = data * self.hold
        # 按照每一个调仓日，调整账户的价值
        for i in range(1, len(self.adj_days)):
            ii = self.adj_days[i]

            d = df.index.tolist().index(ii)
            try:
                ni = self.adj_days[i + 1]
                nd = df.index.tolist().index(ni)
                df.iloc[d: nd - 1] = df.iloc[d: nd - 1] * df.iloc[d - 1].sum()
            except IndexError:
                df.iloc[d:] = df.iloc[d:] * df.iloc[d - 1].sum()

        self.portfolio_value = df
        return df

    @staticmethod
    def _reformater_(data: pd.DataFrame):
        secs = data.code.drop_duplicates().tolist()
        indexes = data.date.drop_duplicates().tolist()
        df_re = pd.DataFrame(columns=secs + ['cash'], index=indexes)

        for s in secs:
            tmp = data.loc[(data.code == s), ['date', 'close']]
            tmp.index = tmp.date.tolist();
            tmp.drop(labels='date', axis=1, inplace=True)
            df_re.loc[:, s] = tmp.values
        df_re.loc[:, 'cash'] = 1
        return df_re
