import numpy as np
import pandas as pd
import pymongo
import datetime
from collections import OrderedDict


class PriceSector:

    def __init__(self, host="LocalHost", port=27017, db="daily", collection="daily"):
        # 单独存一个client， 方便运行完了client.close()
        self.client = pymongo.MongoClient(host=host, port=port)
        conndb = self.client.get_database(name=db)
        # self.conn用来直接存和daily collection的连接
        self.conn = conndb.get_collection(collection)

        # 为了方便查找，从数据库把所有交易日全读出来
        re = conndb.get_collection('tradedays').find(projection={"_id": 0})
        self.tradedays = pd.DataFrame(data=[*re])

    def get_price(self, secs: list, start_date=None, end_date=None, count=None, fields=None, raw=False, fq=True):
        """取【日频】数据最重要的一个method 帮助把复杂的MongoDB查询变简单。
        :parameter

        -- secs: list [a python list of target securities.] You should input a list even if only one stock needed.

        -- start_date: str [optional, choose btw count] [start date. you should give a str(ISO format, e.g. "2021-09-26")
        instead of any other formats like datetime.date]

        -- end_date: str [end date]

        -- count: int [optional]  if start_date is set as None, you may fetch give number of days by specifying count.

        -- fields: list or set [optional] if not specified, OCHL and amount, money will be returned.

        -- dividend_ajd: [optional] default=True. If True, price frame is dividend adjusted. If false, raw data.
        I recommend dividend_adj=True, because this is how we store it.
        """
        end_date = pd.to_datetime(end_date)
        if count is not None:
            start_date = self.tradedays.loc[self.tradedays.date<=end_date].iloc[-count:].iloc[0, 0]
        else:
            start_date = pd.to_datetime(start_date)

        cursor = self.__get_data__(date_instructor='date', start_date=start_date,
                                   end_date=end_date, secs=secs, conn=self.conn, fields=fields, fq=fq)

        # 把取来的数据变成DataFrame
        if raw:
            return cursor
        else:
            df = pd.DataFrame(data=cursor)
            if not fq:
                return df
            else:
                fieldset = {'open', 'close', 'high', 'low'}.intersection(df.columns)
                try:
                    df.loc[:, fieldset] = df.loc[:, fieldset].values * np.stack(
                        [df.loc[:, 'factor'].values]*len(fieldset), axis=1)
                except ValueError:
                    pass
                return df


    @staticmethod
    def __get_data__(date_instructor, start_date: pd._libs.tslibs.timestamps.Timestamp,
                     end_date: pd._libs.tslibs.timestamps.Timestamp,
                     secs: list, conn: pymongo.collection.Collection, fields, fq: bool):
        # 我们不要_id字段，直接先扔掉
        fetch_dict = OrderedDict({"_id": 0})
        possible_fields = {'code', 'date', 'open', 'close', 'high', 'low', 'volume', 'money', 'factor'}
        if fields is not None:
            # 保证fields里没有怪东西 和正常的intersect一下
            fields = set(fields).intersection(possible_fields)
            if len(fields) == 0:
                raise Exception("请检查fields参数， 应该是['code', 'open', 'close', 'high', 'low', 'volume', "
                                "'money', 'factor']其中的几个。 并且以list或者set输入！！！"
                                "Please check your input param fields!!!")
            # for fld in fields:          这个太他妈慢，还好发现了
            #     fetch_dict[fld] = True
            fetch_dict.update(zip(fields, [True]*len(fields)))  # 和上面等效
        else:    # 没有给fields，全取
            fetch_dict.update(zip(possible_fields, [True]*len(possible_fields)))

        fetch_dict['factor'] = fq

        # 下面正式开始取数据
        cursor = conn.find({
            "code": {"$in": secs},
            date_instructor: {"$gte": start_date, "$lte": end_date}},
            projection=fetch_dict,
            cursor_type=pymongo.cursor.CursorType.EXHAUST
        )
        return cursor


class MinutePrice:

    def __init__(self, host='LocalHost', port=27017):
        self.db = pymongo.MongoClient(host=host, port=port).get_database('minute')

        # 把可取的交易日列表做成对象属性  是一个list里边全是日期str
        self.tradedays = [i['available_tradedays'] for i in self.db.get_collection(
            'available_tradedays').find(projection={'_id': 0})]

    def get_minute(self, secs: list, start_date=None, end_date=None, count=None, fields=None,
                   raw=False, dividend_adj=True):
        # 因为我们的分钟数据是分日存的，所以必须先调用__split_days__方法来获取一个list，确定要在哪些collections里取数据
        days = self.__split_days__(start_date=start_date[:10], end_date=end_date[:10])
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        cursors = []

        # 第一天的 要从开始来搞
        for d in __split_days2__(start_date, end_date, days=list(days)):
            col = self.db.get_collection('d'+d[0].strftime("%Y-%m-%d"))
            cur = PriceSector.__get_data__(date_instructor='datetime',
                               start_date=d[0], end_date=d[1], secs=secs, conn=col, fields=fields)
            print(cur.count())
            cursors.extend(list(cur))

        df = pd.DataFrame(data=cursors)
        return df



    def __split_days__(self, start_date, end_date):
        span = set([i.strftime("%Y-%m-%d") for i in pd.date_range(start=start_date, end=end_date)])
        return set(self.tradedays).intersection(span)




def __split_days2__(start_date, end_date, days:list):
    re = []
    if len(days) == 1:
        return [(start_date, end_date)]
    else:
        re.append((start_date, pd.to_datetime(days[0] + 'T15:00:00')))
        try:
            for d in days[2:-1]:
                re.append((pd.to_datetime(d+'T09:30:00'), pd.to_datetime(d+'T15:00:00')))
        finally:
            re.append((pd.to_datetime(days[-1]+'T09:30:00'), end_date))

        return re


if __name__ == '__main__':
    a = PriceSector()
    df = a.get_price(['002603.XSHE'], end_date='2012-01-06', count=20, fq=True)
    print(df)

