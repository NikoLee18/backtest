# Oliver the Cat- Parallel Backtest Framework

I wrote a framework using python in 2020 to do backtest for quantitative investment strategies. However, it turned out, in testing, that my first framework was not a good practice. 

Therein lies myriad of problems. First, it implements strategies trade-day by trade-day, so GIL of python greatly confines how we can boost this process. Next, I attempted to finish calculation of some basic indicators of account (and strategy) as the framework working out investment decisions, so that we can add stopping-loss to it. But this try implies that we are doing some heavy numerical work serially, which should have been done in a vectorized method. 

The biggest problem is the prive database. By the time I wrote 1st edition of backtest code, I had no knowledge of high-performance databases like MongoDB so that version exploits a pandas.DataFrame object to fulfill CRUD functions of a database. 

In this 2nd edition, I fixed all the problems mentioned. And followings are some brief introduction.

## Quick Start
- write a strategy in the main.py file
- run it and get the result
