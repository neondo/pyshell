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


def handler_mobile(e):
    market_mobile = StringUtil.replace_unicode(str(StringUtil.trim(e[0])))
    if market_mobile.find(" ") > 0:
        split = StringUtil.split(market_mobile)
        market_mobile = split[0]
    market_mobile = StringUtil.float2str(market_mobile)
    if not StringUtil.not_null(market_mobile):
        return
    select = db.select_one("SELECT user_id FROM t_cardealer where mobile=%s" % market_mobile)
    if select is None:
        return
    get = select.get("user_id")
    if get is None:
        return
    shop = db_t.select_one("SELECT id from t_market_shop where extend=%s" % get)
    if shop is None or shop.get("id") is None or shop.get("id") == "":
        return
    extend = {"licenceName": e[6], "cooperate": e[8], "cooperateTime": e[9], "link_rank": e[11]}
    ele = {'id': shop["id"], "address": StringUtil.trim(e[4]), "name": e[5], "carport": StringUtil.float2str(e[7]),
           "link_name": e[10], "link_mobile": StringUtil.float2str(e[12]),
           "extend": json.dumps(extend, ensure_ascii=False)}
    return ele


def handler_mobiles(e_arr):
    obj = {}
    for e in e_arr:
        market_mobile = StringUtil.replace_unicode(str(StringUtil.trim(e[0])))
        if not StringUtil.not_null(market_mobile):
            continue
        split = re.split(r"\s+", market_mobile)
        market_mobile = split[0]
        market_mobile = StringUtil.float2str(market_mobile)
        market_mobile = StringUtil.trim(market_mobile)
        obj[market_mobile] = e
    if len(obj) < 1: return
    select = db.select(
        "SELECT user_id,mobile FROM t_cardealer where mobile IN(%s)" % (",".join(f"'{str(i)}'" for i in obj.keys())))
    user_id_arr = {}
    for k in select:
        k_get = k.get("user_id")
        if k_get is None: continue
        user_id_arr.update({str(k_get): k["mobile"]})
    if len(select) < 1: return
    shop = db_t.select(
        "SELECT id,extend user_id from t_market_shop where extend IN (%s)" % (",".join(user_id_arr.keys())))
    update_arr = []
    for e in shop:
        data_e = obj.get(user_id_arr.get(e.get("user_id")))
        if data_e is None: continue
        extend = {"licenceName": data_e[6], "cooperate": data_e[8],
                  "cooperateTime": data_e[9], "link_rank": data_e[11]}
        float_str = StringUtil.float2str(data_e[12])
        if len(str(float_str)) > 11: float_str = str(float_str)[0:11]
        ele = {'id': e["id"],
               "address": StringUtil.trim(data_e[4]),
               "name": data_e[5],
               "carport": StringUtil.float2str(data_e[7]),
               "link_name": data_e[10],
               "link_mobile": float_str,
               "extend": json.dumps(extend, ensure_ascii=False)}
        update_arr.append(ele)
    return update_arr


def update_user_market():
    datalist = ExcelUtil.read(
        r"C:\Users\Neon\Documents\WeChat Files\wxid_8468304683212\FileStorage\File\2020-09\车商 明细.xlsx", "车商明细", 1)
    data_len = len(datalist)
    pool_size = 20
    pool = Pool(pool_size)
    size = math.ceil(float(data_len) / pool_size)
    data_arr = []
    for i in range(pool_size):
        data_arr.append(datalist[i * size:(i + 1) * size])
    pool_map = pool.map_async(handler_mobiles, data_arr)
    result_data = pool_map.get()
    result_data = filter(lambda k: k is not None and len(k) > 0, result_data)
    pool.close()
    pool.join()
    data_sql = []
    for f in result_data:
        f = StringUtil.trimall(f)
        single = ConsistSql.batch_update_single("t_market_shop", f)
        data_sql.append(single)
    FileUtil.write(r"D:\shop_info_update_sql.sql", data_sql)


def user_market_id():
    select = db_t.select("SELECT car_market_id,link_mobile mobile FROM t_market_shop where extend LIKE '%{%'")
    data = []
    user_mobile = {}
    for e in select:
        if re.match(r"\d{11}", e.get("id")):
            user_mobile.update({str(e.get("mobile")): e.get("car_market_id")})
            db_select = db.select(
                "SELECT user_id,mobile FROM t_cardealer where mobile IN(%s)" % (",".join(user_mobile.keys())))
            for e in db_select:
                market_id = user_mobile.get(e.get("mobile"))
                if market_id is None: continue
                data.append({"id": e.get("user_id"), "car_market_id": market_id})

    single = ConsistSql.batch_update_single("t_user", data)
    FileUtil.write(r"D:\user_update_car_market_id_2.sql", single)


if __name__ == "__main__":
    user_market_id()

    # market()
    # inset_market()
    # update_user_detail_market_shop()
