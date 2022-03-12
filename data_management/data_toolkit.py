import os
import pymongo
import multiprocessing as mp
import pandas as pd
from tqdm import tqdm
import jqdatasdk as jq

def get_index_constituents(start_date:str, end_date:str, indexes=['000300.XSHG', '000905.XSHG', '000852.XSHG']):
    """
    取出过去某一段时间的，给定的指数的成分股    用来确定我们的数据库要包含哪些个股
    >>依赖 JoinQuantSDK python module jqdatasdk <<
    """
    alldates = jq.get_index_stocks(start_date, end_date)

    consti = []        # consti list用来存
    for i in indexes:
        for d in tqdm(alldates):
            consti.extend(jq.get_index_stocks(index_symbol=i, date=d))


def reformat_n_insert_jq_downloaded_data(files: list, path: str, target_col: str, target_db: str, compression='gzip',
                                         host='LocalHost', port=27017, require_code=(True, 0), **kwargs):

    if 'self_col' in kwargs.keys():
        col= kwargs['self_col']
    else:
        client = pymongo.MongoClient(host=host,port=port)
        col = client.get_database(target_db).get_collection(target_col)

    for f in tqdm(files):
        df = pd.read_pickle(path + f + '.pkl', compression='gzip')
        # 为了节约空间 把pause改为bool值
        df.loc[df.paused < 0.5, 'paused'] = False
        df.loc[df.paused > 0.5, 'paused'] = True
        if require_code[0]:
            df.insert(require_code[1], column='code', value=[f] * df.shape[0])

        # 下面的用来reset columns name, 也就是说最后的columns是 datetime, code, open, close, high, low, money, volume, factor
        colu = ['datetime']
        colu.extend(df.columns.tolist())

        # 从dataframe里把相应日子的数据取出来
        df.reset_index(inplace=True)

        df.columns = colu

        # 按行提取数据，然后把values插入mongodb
        data = df.to_dict(orient='index')
        col.insert_many(list(data.values()))


def tsfr_pool_data_to_day_specified(collections:list, db:str, source_col:str, host='LocalHost', port=27017, **kwargs):
    """
    STEP 2
    这个函数用来把白嫖到的，混在一起的数据按 天 来分类并插入到相应的collection
    这个函数会把原来的数据原封不动地转移（不会改变列）
    --------------------------------------------------------------------------------------------------------------------
    parameters:

    alldates: 要进行处理的所有日期。 python builtin list

    db: 对应的数据库db名称

    source_col: pooled data 在前面明确的数据库的collection名称

    host: MongoDB Host

    port: MongoDB Port

    self_db: [optional] 如果你要自己传入一个pymongo connection， 就在这里传入pymongo的 db 对象。

    --------------------------------------------------------------------------------------------------------------------

    """
    # first connect to target MongoDB db
    if 'self_db' in kwargs.keys():
        db = kwargs['self_db']
    else:
        db = __create_internal_db_conn(db=db, host=host, port=port)
    fixed_col = db.get_collection(source_col)

    for d in tqdm(collections):
        # create connection to MongoDB collection to WRITE INTO
        d_str = d.strftime('%Y-%m-%d')
        col = db.get_collection('d' + d_str)

        # 查找对应日期
        re = fixed_col.find({'datetime': {
            '$gte': pd.to_datetime(d_str + 'T09:00:00'),
            '$lte': pd.to_datetime(d_str + 'T16:00:00')
        }})
        # fetch batch data from MongoDB
        re = list(re)
        # insert into target collection (day-specified)
        col.insert_many(re)


def batch_ensureIndex(collections: list, index: list, db: str, host='LocalHost', port=27017):
    """
    用来给某个db里的collections全部加上索引
    """
    db = __create_internal_db_conn(db=db, host=host, port=port)
    allcol = db.list_collection_names()
    for c in tqdm(allcol):
        col = db.get_collection(c)
        col.ensure_index(index)



def __create_internal_db_conn(db: str, host='LocalHost', port=27017):
    """
    开一个只在内部用的pymongo和MongoDB连接的对象并返回
    """
    client = pymongo.MongoClient(host=host, port=port)
    db = client.get_database(db)
    return db
