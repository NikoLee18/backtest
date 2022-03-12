def task_distributor(num: int, run_dt: list):
    """
    用来把任务分配到合适数量的进程。
    :param num: 要用到多少个进程
    :param run_dt: 所有需要运行策略的日期
    :return: a list of seperated run dates like: [["2021-01-04","2021-01-05"], ["2021-01-06", "2021-01-07"]]

    """
    re0 = []
    trunc = int(len(run_dt)/num)
    # 先只弄到num-1 个， 最后一个单独来
    for i in range(num-1):
        re0.append(run_dt[trunc * i: trunc * (i+1)])
    # 把最后一个append进来
    re0.append(run_dt[trunc * num: trunc * (num+1)])

    return re0
