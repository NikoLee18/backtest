import pandas as pd
import numpy as np
from statsmodels.api import OLS
from statsmodels.api import add_constant
import matplotlib.pyplot as plt


def getret(ser: pd.Series) -> pd.Series:
    """
    receive an account value series (list) and return a pandas.Series of portfolio returns
    :param ptfl: portfolio total value
    :return:
    """
    return ser.pct_change().dropna()


def alpha_beta(ptfl: pd.Series, bchmk: pd.Series, daily_rf=0) -> tuple:
    """
    This function is used to calculate alpha (in CAPM)
    Here I recommend daily data instead of high-freq series.
    :param ptfl: portfolio total value,  list
    :param bchmk: benchmark value list
    :param daily_rf: the risk free investment return on a daily basis
    :return: alpha, a float (or its counterpart in module numpy
            and beta  ( first return is alpha, second is beta 返回的第一个是alpha， 第二个是beta）
    """
    ptfl = getret(ptfl) - daily_rf
    bchmk = getret(bchmk)
    # check consistency of two series
    index_check(ptfl, bchmk)
    # add constant in regression
    bchmk = add_constant(bchmk)
    ols = OLS(endog=ptfl, exog=bchmk)
    ols = ols.fit()
    return ols.params[0], ols.params[1]


def active_alpha(ptfl: pd.Series, bchmk: pd.Series) -> float:
    """calculate the 'alpha' that is often referred in active portfolio management (in fact, excess return)
    :param ptfl portfolio total value sequence
    :param bchmk benchmark value sequence
    :return active alpha
    """
    index_check(ptfl, bchmk)
    return ptfl.iloc[-1] / ptfl.iloc[0] - bchmk.iloc[-1] / bchmk.iloc[0]


def IR(ptfl: pd.Series, bchmk: pd.Series) -> float:
    """
    Information Ratio 组合的信息比率
    :param ptfl: A list, portfolio return series.
    :param bchmk: A list, benchmark return series
    :return: a float, IR ratio of given portfolio return
    """
    index_check(ptfl, bchmk)
    trak_err = getret(ptfl) - getret(bchmk)
    return active_alpha(ptfl, bchmk) / trak_err.std()


def max_drawdown(ptfl: pd.Series):
    """
    Maximum drawdon: the maximum observed loss from a peak to a trough of a portfolio
    :param ptfl: portfolio total value series
    :return: 2 params. 1st: max_drawdown, 2nd: max drawdown period (an int/float, the number of tradedays in that
    loss period
    """
    cummax = ptfl.cummax()                              # cummax 存下累积的最大值
    d2cummax = ptfl - cummax                            # d2cummax 存每一个时间点收益与累计最大收益的差距， 差距最大的点就是最大回撤
    argmin = d2cummax.argmin()                          # argmin is the location of maximum drawdown (iloc)
    mdd_st = len(d2cummax.where(d2cummax==0).dropna())   # mdd_st is the location of start time of maximum drawdown
    return d2cummax.iloc[argmin], d2cummax.index[mdd_st-1:argmin+1]


def annual_return(ptfl: pd.Series, default_tradedays=252) -> float:
    """
    This function is used to calculate annualized return.
    :param ptfl: a list, portfolio total value sequence
    :return: a float, annualized return
    """
    return np.power(ptfl.iloc[-1]/ptfl.iloc[0], default_tradedays/len(ptfl)) - 1


def sharpe_ratio(ptfl: pd.Series, daily_rf) -> float:
    """
    calculate sharpe ratio of a portfolio ( portfolio annual return - risk free annual)/ annual volatility
    :param ptfl: portfolio total value sequence
    :param annual_rf: annual risk-free return
    :param default_tradedays tradedays in a year
    :return: sharpe ratio
    """
    ret = getret(ptfl)
    if isinstance(daily_rf, float):
        rf = pd.Series(daily_rf, index=ret.index)
    else:
        rf = daily_rf
    excess = ret - rf
    return excess.sum() / excess.std()


def calmar_ratio(ptfl: pd.Series, default_tradedays=252) -> float:
    """
    calculate calmar ratio: annual return / maximum drawdown
    :param ptfl: portfolio total value sequence
    :return: calmar ratio
    """
    return annual_return(ptfl, default_tradedays=default_tradedays) / max_drawdown(ptfl)[0]


def downside_volatility(ptfl: pd.Series) -> float:
    """calculate downside volatility of portfolio, i.e. equate all positive returns to 0 in return sequence and
    calculate standard deviation of the new sequence
    :param ptfl: portfolio total value sequence
    :return downside volatility
    """
    ptfl = getret(ptfl)
    ptfl = ptfl.where(ptfl < 0).fillna(value=0)
    return ptfl.std()


def sortino_ratio(ptfl: pd.Series, daily_rf: float, default_tradedays=252) -> float:
    """
    sortino_ratio = (portfolio  / risk-free rate) / downside volatility
    :param ptfl:
    :param annual_rf: annual riskfree rate
    :param default_tradedays: default number of tradedays in a calendar year, 252 by default
    :return: sortino ratio
    """
    return (getret(ptfl) - daily_rf) / downside_volatility(ptfl)


def index_check(s1, s2):
    try:
        assert len(s1.index) == (s1.index == s2.index).sum()
    except ValueError:
        raise Exception('[--backtest platform--] Length of two input Series or DataFrame does not match.')
    except AssertionError:
        raise Exception(
            '[--backtest platform--] Some entries of two input Series or DataFrame does not match. They may differ, '
            'or not properly sorted')


