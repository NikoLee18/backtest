import pandas as pd
import numpy as np
from statsmodels.api import OLS
from statsmodels.api import add_constant


def getret(ptfl: list) -> pd.Series:
    """
    receive an account value series (list) and return a pandas.Series of portfolio returns
    :param ptfl: portfolio total value
    :return:
    """
    return pd.Series(ptfl).pct_change()


def alpha_beta(ptfl: list, bchmk: list, daily_rf=0) -> tuple:
    """
    This function is used to calculate alpha (in CAPM)
    Here I recommend daily data instead of high-freq series.
    :param ptfl: portfolio total value,  list
    :param bchmk: benchmark value list
    :param daily_rf: the risk free investment return on a daily basis
    :return: alpha, a float (or its counterpart in module numpy
            and beta  ( first return is alpha, second is beta 返回的第一个是alpha， 第二个是beta）
    """
    ptfl = getret(ptfl) - daily_rf; bchmk = getret(bchmk)
    bchmk = add_constant(bchmk)
    ols = OLS(endog=ptfl, exog=bchmk)
    ols = ols.fit()
    return ols.params[0], ols.params[1]


def active_alpha(ptfl: list, bchmk: list) -> float:
    """calculate the 'alpha' that is often referred in active portfolio management
    :param ptfl portfolio total value sequence
    :param bchmk benchmark value sequence
    :return active alpha
    """
    return ptfl[-1] / ptfl[0] - bchmk[-1] / bchmk[0]


def active_IR(ptfl: list, bchmk: list) -> float:
    """
    Information Ratio 组合的信息比率
    :param ptfl: A list, portfolio return series.
    :param bchmk: A list, benchmark return series
    :return: a float, IR ratio of given portfolio return
    """
    ptfl = pd.Series(ptfl); bchmk = pd.Series(bchmk)
    ptfl = ptfl / ptfl.iloc[0]; bchmk = bchmk / bchmk.iloc[0]
    ptfl = ptfl - bchmk
    return ptfl.iloc[-1] / ptfl.pct_change().std()


def max_drawdown(ptfl: list):
    """
    Maximum drawdon: the maximum observed loss from a peak to a trough of a portfolio
    :param ptfl: portfolio total value series
    :return: 2 params. 1st: max_drawdown, 2nd: max drawdown period (an int/float, the number of tradedays in that
    loss period
    """
    ptfl = pd.Series(data=ptfl, name='portfolio')
    cummax = ptfl.cummax()                              # cummax 存下累积的最大值
    d2cummax = ptfl - cummax                            # d2cummax 存每一个时间点收益与累计最大收益的差距， 差距最大的点就是最大回撤
    idxmin = d2cummax.idxmin()
    return d2cummax.min(), (cummax.iloc[:idxmin+1].idxmax(), idxmin)


def annual_volatility(ptfl: list, default_tradedays=252) -> float:
    """
    calculate annualized volatility ( consider a year as a single observation and get its standard deviation)
    :param ret: a list. return sequence
    :param default_tradedays: number of trade days in a calendar year. Here we use the result in China A-share market:252
    :return: a float, annualized volatility
    """
    return getret(ptfl).std() * np.sqrt(default_tradedays)


def annual_return(ptfl: list, default_tradedays=252) -> float:
    """
    This function is used to calculate annualized return.
    :param ptfl: a list, portfolio total value sequence
    :return: a float, annualized return
    """
    return np.power(ptfl[-1]/ptfl[0], default_tradedays/len(ptfl)) - 1


def sharpe_ratio(ptfl: list, annual_rf=0.0015, default_tradedays=252) -> float:
    """
    calculate sharpe ratio of a portfolio ( portfolio annual return - risk free annual)/ annual volatility
    :param ptfl: portfolio total value sequence
    :param annual_rf: annual risk-free return
    :param default_tradedays tradedays in a year
    :return: sharpe ratio
    """
    return (annual_return(ptfl, default_tradedays) - annual_rf) / (getret(ptfl).std() * np.sqrt(default_tradedays))


def calmar_ratio(ptfl: list, default_tradedays=252) -> float:
    """
    calculate calmar ratio: annual return / maximum drawdown
    :param ptfl: portfolio total value sequence
    :return: calmar ratio
    """
    return annual_return(ptfl, default_tradedays=default_tradedays) / max_drawdown(ptfl)[0]


def stability_of_timeseries(ptfl: list):
    ols = OLS(endog=ptfl, exog=add_constant([i for i in range(len(ptfl))]))
    ols = ols.fit()
    return ols.rsquared


def downside_volatility(ptfl: list) -> float:
    """calculate downside volatility of portfolio, i.e. equate all positive returns to 0 in return sequence and
    calculate standard deviation of the new sequence
    :param ptfl: portfolio total value sequence
    :return downside volatility
    """
    ptfl = getret(ptfl)
    ptfl = ptfl.apply(lambda x: 0 if x > 0 else x)
    return ptfl.std()


def annual_downside_volatility(ptfl: list, default_tradedays=252) -> float:
    """calculate annual downside volatility"""
    return downside_volatility(ptfl=ptfl) * np.sqrt(default_tradedays)


def sortino_ratio(ptfl: list, annual_rf=0.0015, default_tradedays=252) -> float:
    """
    sortino_ratio = (portfolio annual return / risk-free rate) / annual downside volatility
    :param ptfl:
    :param annual_rf: annual riskfree rate
    :param default_tradedays: default number of tradedays in a calendar year, 252 by default
    :return: sortino ratio
    """
    return (annual_return(ptfl)-annual_rf) / annual_downside_volatility(ptfl, default_tradedays=default_tradedays)