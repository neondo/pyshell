# coding=utf-8
import pymysql
import logging
import datetime
import time

logging.basicConfig(level=logging.DEBUG)

conn = pymysql.connect(host="172.17.202.101", port=3308, user="read_only", password="kjbcZtQ1BVv76kpC",
                       database="db_www", charset="utf8")

cur = conn.cursor(pymysql.cursors.DictCursor)


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
result = select("SELECT order_no,order_type,user_id,buy_time FROM t_order FORCE INDEX (buy_time) "
                "WHERE order_status IN(1000400,1104) AND "
                "buy_time BETWEEN '2020-07-31' AND '2020-08-11';")

dt = datetime.datetime.now()
current = dt.strftime("%Y-%m-%d %H:%M:%S")
print("当前时间:%s" % current)

resultArr = []  # 需要更新的数据
resultIds = []
for e in result:


    user_id = e["user_id"]
    # 是否是定制估价-实车检测
    if e['order_type'] == 19:
        r_valuation = select_one("SELECT detection_orderno FROM t_valuation_info_customed WHERE order_no=%s LIMIT 1;",
                                 e['order_no'])
        if r_valuation is None or r_valuation['detection_orderno'] is None:
            continue
    # 当时是否绑定销售
    r = select_one(
        "SELECT own_user_id FROM t_own_user_change_log WHERE user_id=%s AND add_time <=%s ORDER BY id DESC LIMIT 1 ",
        (user_id, e['buy_time']))
    if r is None:
        # 某些老客户,没有进行过换绑操作无记录
        r = select_one(
            "SELECT own_user_id FROM t_seller_user WHERE user_id=%s AND add_time <=%s  LIMIT 1 ",
            (user_id, e['buy_time']))
    # 当时有绑定记录的客户继续校验

        sign_result = validity_sign(user_id)  # 判定是否可标记
        if sign_result:
            r_p = select_one("SELECT id FROM t_performance WHERE order_no=%s", e['order_no'])
            if r_p is None:
                continue
            update_data = (r['own_user_id'], current, r_p['id'])  # 封装更新数据
            if len(resultArr) < 3:
                resultArr.append(update_data)
                resultIds.append(str(r_p['id']))

print("更新操作:总数目:%s条" % len(resultArr))
if len(resultArr) > 0:
    try:
        sql = "UPDATE t_performance SET"
        sql_m = " merit_seller_id=CASE id "
        sql_update = " remark=CASE id "
        for e in resultArr:
            sql_m += " WHEN " + str(e[2]) + " THEN " + str(e[0])
            sql_update += " WHEN " + str(e[2]) + " THEN '" + str(e[1]) + "'"
        sql_m += " END,"
        sql_update += " END"
        sql += sql_m + sql_update + " WHERE id IN(" + (','.join(resultIds)) + ");"
        print("本次执行花费10秒")
        print(str(sql))
    except Exception as err:
        print(err)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
