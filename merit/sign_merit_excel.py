# coding=utf-8
import datetime
import openpyxl
import logging

import pymysql
import xlrd
import xlwt
from multiprocessing import Pool
from pandas import DataFrame

logging.basicConfig(level=logging.DEBUG)


def read_excel(path_file):
    with xlrd.open_workbook(path_file) as f:
        return f.sheet_by_name('Sheet1')


def select(sql, arg=None):
    cur.execute(sql, arg)
    return cur.fetchall()


def select_one(sql, arg=None):
    cur.execute(sql, arg)
    return cur.fetchone()


# 判断是否需要标记
def validity_sign(sign_user_id):
    # 是否提单
    n = select_one("SELECT id FROM t_seller_recharge WHERE user_id=%s AND pass_time<NOW()", sign_user_id)
    if n is None:
        # 无提单则查看是否有划转
        r_transfer = select_one("SELECT pay_user_id FROM t_user_money_transfer "
                                "WHERE receipt_user_id=%s", sign_user_id)
        # 无划转返回False 有划转则继续校验划转用户是否提单
        return False if r_transfer is None else validity_sign(r_transfer['pay_user_id'])
    else:
        return True


# 获取所有订单

dt = datetime.datetime.now()
current = dt.strftime("%Y-%m-%d %H:%M:%S")
print("当前时间:%s" % current)
path = "C:\\Users\\Neon\\Documents\\WeChat Files\\wxid_8468304683212\\FileStorage\\File\\2020-08\\永增-业绩系数.xlsx"
resultArr = read_excel(path)
excel_data = []
for i in range(resultArr.nrows):
    e = resultArr.row_values(i)
    if i == 0:
        excel_data.append(e)
        continue
    user_id = e[0]
    # 是否是定制估价-实车检测
    time_ = e[3]
    order_type = e[4]
    order_no_ = e[2]
    own_user_id = None
    own_user_name = None

    if order_type == '定制估价':
        r_valuation = select_one("SELECT detection_orderno FROM t_valuation_info_customed WHERE order_no=%s LIMIT 1;",
                                 order_no_)
        if r_valuation is None or r_valuation['detection_orderno'] is None:
            continue
    # 当时是否绑定销售
    r = select_one(
        "SELECT own_user_id,own_user_name FROM t_own_user_change_log WHERE type=0 "
        "AND user_id=%s AND add_time <=%s ORDER BY id DESC LIMIT 1 ",
        (user_id, time_))
    if r is None:
        # 某些老客户,没有进行过换绑操作无记录
        r = select_one(
            "SELECT own_user_id FROM t_seller_user WHERE user_id=%s AND add_time <=%s  LIMIT 1 ",
            (user_id, time_))
    else:
        own_user_name = r['own_user_name']

    # 当时有绑定记录的客户继续校验
    if r is not None and r['own_user_id'] is not None:
        own_user_id = r['own_user_id']

        sign_result = validity_sign(user_id)  # 判定是否可标记
        if sign_result:
            e[1] = own_user_id
            e[2] = own_user_name
            excel_data.append(e)
            print(str(e))

workbook = xlwt.Workbook(encoding='utf-8')
worksheet = workbook.add_sheet('My Worksheet')
print("starting...")
frame = DataFrame(excel_data)
frame.to_excel(r'writexcel.xlsx', index=False)
print("quite...")
