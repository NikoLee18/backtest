import pandas as pd


class Position:
    """This class is a modified version class of target, which uses pandas.DataFrame to log positions and perform better
    when calculating performance of a portfolio.
    For every new trade day, method update HAVE TO BE RUN ->FIRST<- to refresh locked securities"""

    def __init__(self):
        # 这里time一栏主要是给后续加入分钟高频回测使用的。  如果日频测试股票就直接time填充datetime.time(0,0).
        self.position = pd.DataFrame(columns=['symbol', 'amount', 'locked_amount',
                                              'total_cost', 'avg_cost'])

    def buy(self, code, amount, deal_price):
        """买入标的物
            返回账户->减少<-的资金数量（只返回正值，调用这个方法时候记得加上负号）"""
        if code not in self.position.symbol.values:
            self.position = self.position.append(
                {'symbol': code, 'amount': amount, 'locked_amount': amount, 'total_cost': amount * deal_price,
                 'avg_cost': deal_price}, ignore_index=True)
            # then we return the required cash
            return amount * deal_price
        else:
            targetline = self.position[self.position.symbol == code]
            # drop the target (we will then concat new info to it)
            self.position.drop(index=self.position[self.position.symbol == code].index, inplace=True)
            # 更新锁定数量（T+1）,如果要T+0直接在main里忽略掉这项即可
            targetline['locked_amount'].values[0] = targetline['locked_amount'].values[0] + amount
            # 更新 累计总成本
            targetline['total_cost'].values[0] = targetline['total_cost'].values[0] + deal_price * amount
            # 更新 平均持仓成本 用于计算浮动盈亏
            targetline['avg_cost'].values[0] = (targetline['avg_cost'].values[0] * targetline['amount'].values[
                0] + deal_price * amount) / (targetline['amount'].values[0] + amount)
            # 更新持仓数量
            targetline['amount'].values[0] = targetline['amount'].values[0] + amount
            # concat changed position info to original DataFrame
            self.position = self.position.append(targetline, ignore_index=True)
            # return required cash
            return amount * deal_price

            # log this trade 后续加入该功能

    def sell(self, code, amount, deal_price):
        """卖出已经持有标的
            返回账户增加的可用资金数量"""

        # we do this in a 'try-exception' style to prevent Error
        try:
            targetline = self.position[self.position.symbol == code]

            if amount <= targetline['amount'].values[0] - targetline['locked_amount'].values[0]:
                # drop the target (we will then concat new info to it)
                self.position.drop(index=self.position[self.position.symbol == code].index, inplace=True)

                # 更新 累计总成本
                targetline['total_cost'].values[0] = targetline['total_cost'].values[0] - deal_price * amount
                # 更新持仓数量
                targetline['amount'].values[0] = targetline['amount'].values[0] - amount
                # 计算账户可用资金增加额度
                incremental = amount * deal_price  # return this digit in the end
            else:
                # 要卖的券数量大于可卖， 不做任何操作 账户资金增加量为0， 返回0
                return 0
            # concat changed position info to original DataFrame
            self.position = self.position.append(targetline, ignore_index=True)
            return incremental

        except IndexError:
            return 0  # return an abnormaly large number
        except:
            raise Exception("检查源代码！")

    def refresh(self):
        """每日开始时运行,把锁定仓位解锁"""
        self.position.loc[:, ['locked_amount']] = [0] * self.position.shape[0]

    def get_float_return(self, price_series):
        """获取浮动盈亏
            传入：pd.Series,index:stock symbol, values:current_price
            e.g. price_series: 000001.XSHE 23
                               000789.XSHE 34
                               510300.XSHG 5.34...
            返回：pd.Series,index:stock symbol, values:total float return"""
        # get a copy of attribute self.position and reformat it
        cp_position = self.position.copy()
        cp_position.set_index(keys='symbol', drop=True, inplace=True)

        cp_position = pd.concat([cp_position, price_series], axis=1)
        cp_position['float_ret'] = (cp_position.loc[:, 0] - cp_position.loc[:, 'avg_cost']) * cp_position.loc[:,
                                                                                              'amount']

        return pd.Series(cp_position.float_ret, index=cp_position.index)

    def get_value(self, price_series):
        """与 get_float_return相似，用于获取持仓股票价值（一个pandas.Series,index为代码，values为总价值）"""
        cp_position = self.position.copy()
        cp_position.set_index(keys='symbol', drop=True, inplace=True)

        cp_position = pd.concat([cp_position, price_series], axis=1)
        cp_position['float_value'] = cp_position.loc[:, 0] * cp_position.loc[:, 'amount']

        return pd.Series(cp_position.float_value, index=cp_position.index)


class ShortPosition:
    """This class is a modified version class of target, which uses pandas.DataFrame to log positions and perform better
    when calculating performance of a portfolio.
    For every new trade day, method update HAVE TO BE RUN ->FIRST<- to refresh locked securities
    -->真实<-- 1.融券还券后次日解锁保证金 2.维持担保比例（保证金/债务）超过300%部分保证金解锁
    -->目前功能<-- 1.还券当日解锁保证金 2.还券/还钱之前不解锁保证金
    __init__()需要传入margin_ratio,默认为1（100%保证金）"""

    def __init__(self, margin_ratio=1):
        self.margin_ratio = margin_ratio
        self.position = pd.DataFrame(columns=['symbol', 'amount', 'deposit', 'total_cost', 'avg_cost'])

    def cover_short(self, code, amount, deal_price):
        """买券还券,返回的数值为账户内应该->增加<-的可用资金,i.e.解锁保证金-买券所需资金"""
        # we do this in a 'try-exception' style to prevent Error
        try:
            targetline = self.position[self.position.symbol == code]
            # drop the target (we will then concat new info to it)
            self.position.drop(index=self.position[self.position.symbol == code].index, inplace=True)
            if amount <= targetline['amount'].values[0]:
                # 更新 累计总成本
                targetline['total_cost'].values[0] = targetline['total_cost'].values[0] - deal_price * amount
                # 更新空头仓位
                targetline['amount'].values[0] = targetline['amount'].values[0] - amount
                # ->解锁保证金<-
                targetline['deposit'].values[0] = targetline['deposit'].values[0] - targetline['avg_cost'].values[
                    0] * amount * self.margin_ratio
                # 记录返回账户内应该->增加<-的资金，i.e.解锁保证金-买券所需资金
                incremental = targetline['avg_cost'].values[0] * amount * self.margin_ratio - amount * deal_price
            else:
                targetline['total_cost'].values[0] = (
                        targetline['total_cost'].values[0] - deal_price * targetline['amount'].values[0])
                targetline['amount'].values[0] = 0
                # ->解锁保证金,并返回解锁额度<-
                targetline['deposit'].values[0] = 0
                # 记录返回账户内应该->增加<-的资金，i.e.解锁保证金-买券所需资金
                incremental = targetline['avg_cost'].values[0] * targetline['amount'].values[0] * self.margin_ratio - \
                              targetline['amount'].values[0] * deal_price
            # concat changed position info to original DataFrame
            self.position = self.position.append(targetline, ignore_index=True)
            # return incremental
            return incremental
        except IndexError:
            return 0

    def sell_short(self, code, amount, deal_price):
        """卖空标的物，返回的数值为账户内->增加<-的资金"""
        if code not in self.position.symbol.values:
            self.position = self.position.append(
                {'symbol': code, 'amount': amount, 'deposit': amount * deal_price * self.margin_ratio,
                 'total_cost': amount * deal_price,
                 'avg_cost': deal_price}, ignore_index=True)
            # 返回账户增加的资金,如果返回的是正值代表保证金比例低于1，返回0代表100%保证金，小于0代表做空需要超过100%保证金
            return deal_price * amount * (1 - self.margin_ratio)
        else:
            targetline = self.position[self.position.symbol == code]
            # drop the target (we will then concat new info to it)
            self.position.drop(index=self.position[self.position.symbol == code].index, inplace=True)
            # 记录保证金占用额度
            targetline['deposit'].values[0] = targetline['deposit'].values[0] + amount * deal_price * self.margin_ratio
            # 更新 累计总成本（该“成本”意义与多头不同）
            targetline['total_cost'].values[0] = targetline['total_cost'].values[0] + deal_price * amount
            # 更新 平均持仓成本 用于计算浮动盈亏
            targetline['avg_cost'].values[0] = (targetline['avg_cost'].values[0] * targetline['amount'].values[
                0] + deal_price * amount) / (targetline['amount'].values[0] + amount)
            # 更新持仓数量
            targetline['amount'].values[0] = targetline['amount'].values[0] + amount
            # concat changed position info to original DataFrame
            self.position = self.position.append(targetline, ignore_index=True)

            # 返回账户增加的资金,如果返回的是正值代表保证金比例低于1，返回0代表100%保证金，小于0代表做空需要超过100%保证金
            return deal_price * amount * (1 - self.margin_ratio)

        #     refresh方法在后续更新加入解锁/追缴保证金后开通

    #     def refresh(self):
    #         """每日开始时运行,把锁定仓位解锁"""
    #         self.position.loc[:, ['locked_amount']] = [0] * self.position.shape[0]

    def get_float_return(self, price_series):
        """获取浮动盈亏
            传入：pd.Series,index:stock symbol, values:current_price
            e.g. price_series: 000001.XSHE 23
                               000789.XSHE 34
                               510300.XSHG 5.34...
            返回：pd.Series,index:stock symbol, values:total float return"""
        # get a copy of attribute self.position and reformat it
        cp_position = self.position.copy()
        cp_position.set_index(keys='symbol', drop=True, inplace=True)

        cp_position = pd.concat([cp_position, price_series], axis=1)
        cp_position['float_ret'] = (cp_position.loc[:, 'avg_cost'] -
                                    cp_position.loc[:, 0]) * cp_position.loc[:, 'amount']

        return pd.Series(cp_position.float_ret, index=cp_position.index)

    def get_value(self, price_series):
        """very similar to method get_float_return, but return the incremental value """
        # get a copy of attribute self.position and reformat it
        cp_position = self.position.copy()
        cp_position.set_index(keys='symbol', drop=True, inplace=True)

        cp_position = pd.concat([cp_position, price_series], axis=1)
        cp_position['float_value'] = ((cp_position.loc[:, 'avg_cost'] - cp_position.loc[:, 0]) *
                                      cp_position.loc[:, 'amount'] + cp_position.loc[:, 'deposit']
                                      - price_series * cp_position.loc[:, 'amount'])
        # ~~speculate gain          ~~ cash needed to buy back securities           ~~return deposit
        return pd.Series(cp_position.float_value, index=cp_position.index)

    def refresh(self):
        """每日开始时运行,把锁定仓位解锁"""
        self.position.loc[:, ['locked_amount']] = [0] * self.position.shape[0]




