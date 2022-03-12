import numpy as np
from collections import OrderedDict
import pandas as pd
from PriceSector import PriceSector
import datetime


class NormalClass:

    one = 1
    thousand = 1000
    million = 1000000



class ParaPosition:
    """This class is a modified version class of target, which uses pandas.DataFrame to log positions and perform better
    when calculating performance of a portfolio.
    For every new trade day, method update HAVE TO BE RUN ->FIRST<- to refresh locked securities
    --------------------------------------------------------------------------------------------------------------------
    2022更新：并行版本
    每次打开会建立一个和mongoDB数据库的连接
    并行版本无法运行有路径依赖的策略，比如带>>止损<<的策略
    --------------------------------------------------------------------------------------------------------------------
    """

    def __init__(self, date: datetime.datetime,host="LocalHost", port=27017, db="daily", collection="daily",
                 normalization='thousand'):
        # 只记录需要的仓位就好。 后边计算收益的时候前后做差
        self.position = pd.DataFrame(columns=['code', 'amount', 'total_cost', 'avg_cost'], dtype=np.float64)
        self.date = date
        # 重点！ 建立和数据库的接口
        self.pr = PriceSector(host=host, port=port, db=db, collection=collection)

        assert normalization in NormalClass.__dict__
        self.normalization_scale = NormalClass.__dict__[normalization]
        self.to_execute_cache = []

    def target_amount(self, code: list, amount: dict, order_time='open'):
        """
        按照需要的股票数量买入（*由于并行测试，这里的股票数量仅保证相对比例，无法保证绝对数值）
        :param code:
        :param amount:
        :param order_time:
        :return:
        """
        if order_time in ['open', 'close']:
            pr = self.pr.get_price(secs=code, end_date=self.date, count=1, fields=['code', order_time])
        else:
            raise Exception("目前只支持按照开盘价或者收盘价买卖，日内选择时间的方法后续加入")
        for stk in code:
            self._execute_(code=stk, amount=amount[stk], deal_price=pr.loc[pr.code == stk, order_time].values[0])

        # 最后关键一步 进行归一处理 -- update: 转移到get_final_value方法里操作
        # self.__normalize__(proportion)

    def target_value(self, code: list, value: dict, order_time='open'):
        if order_time in ['open', 'close']:
            pr = self.pr.get_price(secs=code, end_date=self.date, count=1, fields=['code', order_time])
        else:
            raise Exception("目前只支持按照开盘价或者收盘价买卖，日内选择时间的方法后续加入")
        for stk in code:
            deal = pr.loc[pr.code == stk, order_time].values[0]
            self._execute_(code=stk, amount=value[stk] / deal, deal_price=deal)

        # 最后关键一步 进行归一处理  -- update: 转移到get_final_value方法里操作
        # self.__normalize__(proportion)

    def __normalize__(self, proportion: float):
        """
        ！！！ 只能够在准备获取当天的数据时候调用！！！
        :param proportion: 当日使用的仓位（用于进行择时）
        :return:
        """
        proportion = self.normalization_scale * proportion
        sum_value = self.position.loc[:, 'total_cost'].sum()
        #  先把total_cost 更新了
        self.position.loc[:, 'total_cost'] = proportion * self.position.loc[:, 'total_cost'] / sum_value
        # 然后根据单位化的total_cost， 重新计算持仓量
        self.position.loc[:, 'amount'] = self.position.loc[:, 'total_cost'] / self.position.loc[:, 'avg_cost']

    def _execute_(self, code, amount, deal_price):
        """买入标的物
            返回账户->减少<-的资金数量（只返回正值，调用这个方法时候记得加上负号）"""
        # 不管怎么样，先把今天要买入的记录下来, 这样也有利于记录不同价格买入的
        self.to_execute_cache.append(
            {'code': code, 'amount': amount, 'total_cost': amount * deal_price,
             'avg_cost': deal_price}
        )
        # 这里我们不用return价格了，因为position只作为一个记录权重的工具

    def disconnect(self):
        """运行结束时，调用这个方法与数据库解除连接"""
        self.pr.client.close()

    def get_account_info(self, proportion=1.0):
        """运行完策略需要计算收益，return一个dict，返回 >>持仓比例<< """
        self.position = pd.DataFrame(data=self.to_execute_cache)

        self.__normalize__(proportion)
        data = self.position.to_dict()
        # 先生成持仓量的数据
        dt = pd.to_datetime(self.date)
        hold = pd.DataFrame(data=dict(zip(data['code'].values(), data['amount'].values())),
                            index=[dt], dtype=np.float64)
        hold.insert(loc=hold.shape[1], column='pos', value=proportion)
        return hold


class ParaShortPosition:
    """This class is a modified short position logger, which uses pandas.DataFrame to log positions and perform better
    when calculating performance of a portfolio.
    --------------------------------------------------------------------------------------------------------------------
    2022更新：并行版本
    每次打开会建立一个和mongoDB数据库的连接
    并行版本无法运行有路径依赖的策略，比如带>>止损<<的策略.相较于多头仓位有一个警告
    --------------------------------------------------------------------------------------------------------------------
    WARNING:
        Using this class to log short position is not appreciated because in reality short position is subject to
        budget constrains and thus path-dependent, which is not supported in paralleled backtest.

        Therefore, you can not use margin restrictions here.

        You have to make sure account value do not reach 0. Or the pnl calculation in final step will collapse.
    """

    def __init__(self, date: datetime.datetime, host="LocalHost", port=27017, db="daily", collection="daily"):
        # 只记录需要的仓位就好。 后边计算收益的时候前后做差
        self.position = pd.DataFrame(columns=['code', 'amount', 'total_cost', 'avg_cost'])
        self.date = date
        # 重点！ 建立和数据库的接口
        self.pr = PriceSector(host=host, port=port, db=db, collection=collection)

    def target_amount(self, code: list, short_amount: dict, order_time='open'):
        """
        按照需要的股票数量买入（*由于并行测试，这里的股票数量仅保证相对比例，无法保证绝对数值）
        :param code:
        :param amount:
        :param order_time:
        :return:
        """
        if order_time in ['open', 'close']:
            pr = self.pr.get_price(secs=code, end_date=self.date, count=1, fields=['code', order_time])
        else:
            raise Exception("目前只支持按照开盘价或者收盘价买卖，日内选择时间的方法后续加入")
        for stk in code:
            self._buy_(code=stk, amount=short_amount[stk], deal_price=pr.loc[pr.code == stk, order_time].values[0])

    def target_value(self, code: list, short_value: dict, order_time='open'):
        if order_time in ['open', 'close']:
            pr = self.pr.get_price(secs=code, end_date=self.date, count=1, fields=['code', order_time])
        else:
            raise Exception("目前只支持按照开盘价或者收盘价买卖，日内选择时间的方法后续加入")
        for stk in code:
            deal = pr.loc[pr.code == stk, order_time].values[0]
            # short_value / deal price就是空多少
            self._buy_(code=stk, amount=short_value[stk] / deal, deal_price=deal)

    def __normalize__(self, proportion: float):
        sum_value = self.position.loc[:, 'total_cost'].sum()
        self.position.loc[:, 'amount'] = proportion * self.position.loc[:, 'amount'] * \
                                         self.position.loc[:, 'total_cost'] / self.position.loc[:, 'total_cost'].sum()

    def _buy_(self, code, amount, deal_price):
        """买入标的物
            返回账户->减少<-的资金数量（只返回正值，调用这个方法时候记得加上负号）"""
        # if code not in self.position.code.values:
        self.position = self.position.append(
            {'code': code, 'amount': amount, 'total_cost': amount * deal_price,
             'avg_cost': deal_price}, ignore_index=True)
        # 这里我们不用return价格了，因为position只作为一个记录权重的工具

    def disconnect(self):
        """运行结束时，调用这个方法与数据库解除连接"""
        self.pr.client.close()

    def get_account_info(self, proportion=1.0):
        """运行完策略需要计算收益，return一个dict，返回 >>空头持仓比例<< """
        self.__normalize__(proportion)
        data = self.position.to_dict()
        # 先生成持仓量的数据
        hold = pd.DataFrame(data=dict(zip(data['code'].values(), data['amount'].values())),
                            index=[pd.to_datetime(self.date)])
        return hold



if __name__ == '__main__':
    a = ParaPosition(pd.to_datetime('2022-01-06'))
    secs = ['000001.XSHE', '000002.XSHE', '000987.XSHE', '300450.XSHE', '689009.XSHG', '600000.XSHG']
    a.target_value(secs, dict(zip(secs, [100]* len(secs))))
    print(a.get_account_info())

# #
# def get_pnl(path: str):
#     """
#
#     --------------------------------------------------------------------------------------------------------------------
#     parameters:
#
#     path: str. path of output holding info
#
#     """
#     # 初次调仓不用管，从第二次开始调整
#     for i in range(1, len(obj.adj_days)):
#
#         ii = obj.adj_days[i]
#
#         d = df.index.tolist().index(ii)
#         try:
#             ni = obj.adj_days[i + 1]
#             nd = df.index.tolist().index(ni)
#             df.iloc[d: nd - 1] = df.iloc[d: nd - 1] * df.iloc[d - 1].sum()
#         except IndexError:
#             df.iloc[d:] = df.iloc[d:] * df.iloc[d - 1].sum()
#         break
