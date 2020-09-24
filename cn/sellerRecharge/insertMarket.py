# coding=utf-8
import random
from multiprocessing import Pool
import datetime
from functools import partial
from cn.util.util import *
import difflib

import json
import re

db = DbUtil("m")

db_t = DbUtil("t")

if __name__ == "__main__":
    select = db.select("SELECT * FROM t_seller_recharge where add_time>='2020-01-01'")
    arr = []
    for e in select:
        arr.append(str(e.get("own_user_id")))
    t_select = db_t.select("SELECT id FROM t_own_user where id IN(%s)" % (",".join(arr)))
    k = []
    for i in t_select:
        k.append(i.get("id"))
    for e in select:
        del e["id"]
        if e.get("own_user_id") not in k:
            e["own_user_id"] = k[random.randint(0, len(k) - 1)]
    sql = ConsistSql.batch_insert("t_seller_recharge", select)
    db_t.update(sql)
