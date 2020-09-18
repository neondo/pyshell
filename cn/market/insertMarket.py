# coding=utf-8
from multiprocessing import Pool
import datetime
from functools import partial
from cn.util.util import *
import difflib

import json
import re


def get_equal_rate(str1='', str2=''):
    return difflib.SequenceMatcher(None, str1, str2).quick_ratio()


db = DbUtil("m")


def inset_market():
    read = ExcelUtil.read(
        r"C:\Users\Neon\Documents\WeChat Files\wxid_8468304683212\FileStorage\File\2020-09\合作市场方(1)(1)(1).xlsx",
        "Sheet1", 1)
    market_list = []
    td = datetime.datetime.now()
    current = td.strftime("%Y-%m-%d %H:%M:%S")
    select_city_result = db.select("SELECT id,name FROM t_province_city")
    city_dict = {}
    for e in select_city_result:
        city_dict.update({e.get("name"): e.get("id")})
    for i, e in enumerate(read, 1):
        city_name = e[3]
        city_id = city_dict.get(city_name + "市", city_dict.get(city_name))
        if type(city_id) != int: continue
        e_ = e[1]
        e_ = e_.replace("\n", ",") if e_ is not None and type(e_) is not float else e_
        extend = {"userId": e_, "cooperateStatus": e[6], "cooperateTime": e[7], "payType": e[8],
                  "carport": e[9], "dealerCount": e[10], "dealerVipCount": e[11], "dealerDetectionCount": e[12]
            , "orderDetectionCount": e[13], "orderWarrantyCount": e[14]}
        s = json.dumps(extend, ensure_ascii=False)
        get = extend.get("carport")
        market_list.append(
            {'id': i, "name": e[4], "address": e[5], "city_id": city_id,
             "status": 1, "extend": s, "add_time": current,
             "parking_space_num": None if get == "" else get})
    insert = ConsistSql.batch_insert("t_car_market", market_list)
    print(insert)


db_t = DbUtil("t")


def update_user_market():
    read = ExcelUtil.read(
        r"C:\Users\Neon\Documents\WeChat Files\wxid_8468304683212\FileStorage\File\2020-09\车商 明细.xlsx", "车商明细", 1)
    update_market_shop = []
    for e in read:
        market_mobile = int_v(e[0])
        if not not_null(market_mobile): continue
        select = db.select_one("SELECT user_id FROM t_cardealer where mobile=%d" % market_mobile)
        if select is None: continue
        get = select.get("user_id")
        if get is None: continue
        shop = db_t.select_one("SELECT id from t_market_shop where extend=%s" % get)
        if shop is None or shop.get("id") is None or shop.get("id") == "": continue
        extend = {"licenceName": e[6], "cooperate": e[8], "cooperateTime": e[9], "link_rank": e[11]}
        ele = {'id': shop["id"], "address": trim(e[4]), "name": e[5], "carport": int_v(e[7]),
               "link_name": e[10], "link_mobile": int_v(e[12]), "extend": json.dumps(extend, ensure_ascii=False)}
        update_market_shop.append(ele)
        if len(update_market_shop) > 30: break
    ConsistSql.batch_update_single("t_market_shop", update_market_shop)


if __name__ == "__main__":
    update_user_market()
# market()
# inset_market()
# update_user_detail_market_shop()
