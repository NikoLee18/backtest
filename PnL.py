import pymongo
import pandas as pd
import os
from para_backtest import NormalClass
from PriceSector import PriceSector
import indicator_toolkit as idt
import openpyxl
import matplotlib.pyplot as plt


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
        # March18Update object method print_result requires benchmark data so startdate and enddate will be added to attr
        self.period = (start_date, end_date)
        # March13修改 改用PriceSector取数据
        self.pr = PriceSector(host=host, port=port)
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
        files = os.listdir(path+'/holding')
        for f in files:
            re.append(pd.read_excel(path + '/holding/' + f, index_col=0))
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
        # March18Update path required in method print_results
        self.path = path

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
        for i in range(1, len(self.adj_days)):  # adjustments taken on 1,2,...,n-1, time 0 does not matter
            ii = self.adj_days[i]

            d = df.index.tolist().index(ii)
            try:
                ni = self.adj_days[i + 1]   # ni for "next index". multiply portfolio holding amount by scaling ratio
                nd = df.index.tolist().index(ni)
                df.iloc[d: nd - 1] = df.iloc[d: nd - 1] * df.iloc[d - 1].sum() / self.normalization
            except IndexError:
                df.iloc[d:] = df.iloc[d:] * df.iloc[d - 1].sum() / self.normalization

        self.portfolio_value = df
        return df

    def print_result(self, benchmark='000300.XSHG', daily_rf=0):
        """
        output backtest results
        :param benchmark:
        :return:
        """
        if not isinstance(self.portfolio_value, pd.DataFrame):
            ser = self.generate_portfolio_ret().sum(axis=1)
        else:
            ser = self.portfolio_value.sum(axis=1)
        # get plot
        # get benchmark
        bm = self.pr.get_price([benchmark], start_date=self.period[0], end_date=self.period[1], fields=['date', 'close'],
                               raw=False, fq=True)
        bm.set_index('date', inplace=True)
        bm.sort_index(inplace=True)
        bm = bm.close
        fig = plot_pnl(ser, bm, initcash=self.normalization)
        fig.savefig(self.path+'/PnL_Plot.png')
        # indicator result
        self.portfolio_value.to_excel(self.path+'/holding_info.xlsx')
        insert_pnl_indicators(ser, bm, self.path+'/holding_info.xlsx', daily_rf=daily_rf,
                              normalization=self.normalization)



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


def pos_generator():
    for i in range(65, 91):
        for j in range(1, 3):
            yield chr(i) + str(j)


def insert_pnl_indicators(ptfl: pd.Series, bm: pd.Series, file, daily_rf=0,
                          normalization=1000, default_tradedays=252):
    wb = openpyxl.load_workbook(file)
    wb.create_sheet(index=1, title='indicator')
    sheet = wb['indicator']

    pos = pos_generator()
    # Ret
    sheet[next(pos)] = 'TotalReturn'
    sheet[next(pos)] = ptfl.iloc[-1] / ptfl.iloc[0]
    # Annual Ret
    sheet[next(pos)] = 'AnnualReturn'
    sheet[next(pos)] = idt.annual_return(ptfl)
    # ExcessRet
    sheet[next(pos)] = 'ExcessReturn'
    sheet[next(pos)] = ptfl.iloc[-1] / ptfl.iloc[0] - bm.iloc[-1] / bm.iloc[0]
    # alpha beta
    a, b = idt.alpha_beta(ptfl, bm)
    sheet[next(pos)] = 'alpha'
    sheet[next(pos)] = a
    sheet[next(pos)] = 'beta'
    sheet[next(pos)] = b
    # sharpe
    sheet[next(pos)] = 'Sharpe'
    sheet[next(pos)] = idt.sharpe_ratio(ptfl, daily_rf)
    # max draw-down
    a, b = idt.max_drawdown(ptfl)
    sheet[next(pos)] = 'MaxDrawdown'
    sheet[next(pos)] = a / normalization
    sheet[next(pos)] = 'DrawdownPeriod'
    sheet[next(pos)] = b[0].strftime("%Y-%m-%d") + "----" + b[-1].strftime("%Y-%m-%d")
    # Calmar
    sheet[next(pos)] = 'Calmar'
    sheet[next(pos)] = idt.calmar_ratio(ptfl, default_tradedays)
    # IR
    sheet[next(pos)] = 'IR'
    sheet[next(pos)] = idt.IR(ptfl, bm)
    # strategy vol
    sheet[next(pos)] = 'StrategyVolatility'
    sheet[next(pos)] = idt.getret(ptfl).std()

    wb.save(file)


def plot_pnl(ser: pd.Series, bm: pd.Series, initcash=1000):
    """
    Plot portfolio PnL, max draw-down and daily ret.
    :param ser:
    :param bm:
    :param initcash:
    :return: matplotlib figure
    """
    _, mdd = idt.max_drawdown(ser)
    fig, ax = plt.subplots(2, 1, figsize=(20, 16), gridspec_kw={'height_ratios': [5, 1]})
    # pnl
    ax[0].plot(ser.loc[:mdd[0]], 'b-', linewidth=3, label='Portfolio Value')
    ax[0].plot(ser.loc[mdd[-1]:], 'b-', linewidth=3)
    ax[0].plot(ser.loc[mdd], linestyle='--', color='grey', linewidth=3)

    ax[0].scatter(mdd[0], ser.loc[mdd[0]], c='orange', s=250)
    ax[0].scatter(mdd[-1], ser.loc[mdd[-1]], c='orange', s=250)

    ax[0].plot(bm / (bm.iloc[0] / ser.iloc[0]), 'r-', label='Benchmark')
    ax[0].grid()
    ax[0].legend(prop={'size': 15})

    # ret
    ax[1].plot(ser.index, [initcash * 0.05] * len(ser), 'm--')
    ax[1].plot(ser.index, [-initcash * 0.05] * len(ser), 'm--')
    ax[1].stem(ser.index.tolist(), ser - ser.shift(1))
    ax[1].grid()

    return fig

