# coding=utf-8
import datetime, time
import logging

from cn.util.util import DbUtil
from multiprocessing import Pool

logging.basicConfig(level=logging.DEBUG)


# 判断是否需要标记
def validity_sign(sign_user_id, success_add_time):
    # 是否提单
    n = DbUtil.select_one("SELECT id FROM t_seller_recharge WHERE user_id=%s AND pass_time<%s",
                          (sign_user_id, success_add_time))
    if n is None:
        # 无提单则查看是否有划转
        r_transfer = DbUtil.select_one("SELECT pay_user_id FROM t_user_money_transfer "
                                       "WHERE receipt_user_id=%s", sign_user_id)
        # 无划转返回False 有划转则继续校验划转用户是否提单
        return False if r_transfer is None else validity_sign(r_transfer['pay_user_id'], success_add_time)
    else:
        return True


resultArr = []  # 需要更新的数据
resultIds = []


def merit_type_order(account_log):
    merit_type = None
    m_user_id = account_log["user_id"]
    account_log_id = account_log["id"]
    cost_type = account_log["cost_type"]
    money = account_log["money"]
    if cost_type in [0, 8, 11]:
        merit_type = 0
    elif 1 == cost_type:
        first_log = DbUtil.select_one("SELECT id FROM t_account_log WHERE user_id=%s"
                                      " AND cost_type=1 ORDER BY id  LIMIT 1" % m_user_id)
        merit_type = 1 if first_log["id"] == account_log_id else 3
    elif 2 == cost_type:
        first_log = DbUtil.select_one("SELECT money FROM t_account_log WHERE user_id=%s"
                                      " AND cost_type=1 ORDER BY id  LIMIT 1" % m_user_id)
        user_account = DbUtil.select_one("SELECT total_cost FROM t_account WHERE user_id=%s;" % m_user_id)
        cost_balance = first_log["money"] - user_account["total_cost"]
        if money < cost_balance:
            merit_type = 5
        else:
            sum_money = DbUtil.select_one(
                "SELECT SUM(money) total_draw FROM t_account_log WHERE user_id=%s AND cost_type=2" % m_user_id)
            merit_type = 5 if cost_balance < sum_money["total_draw"] else 8
    elif cost_type in [3, 5, 9, 10]:
        merit_type = 4
    elif 7 == cost_type:
        merit_type = 4 if money > 0 else 0
    return merit_type


def judge_merit(order_ele):
    user_id_inner = order_ele['user_id']
    order_type_inner = order_ele['order_type']
    buy_time_inner = order_ele['buy_time']
    order_no_ = order_ele['order_no']
    if order_type_inner == 19:
        r_valuation = DbUtil.select_one(
            "SELECT detection_orderno FROM t_valuation_info_customed WHERE order_no=%s LIMIT 1;",
            order_no_)
        if r_valuation is None or r_valuation['detection_orderno'] is None:
            return False
        # 当时有绑定记录的客户继续校验
    sign_result = validity_sign(user_id_inner, buy_time_inner)  # 判定是否可标记
    if sign_result:
        r_p = DbUtil.select_one("SELECT id FROM t_performance WHERE order_no=%s", order_no_)
        if r_p is None:
            return False
        return True


insert_data = []


def operate_merit(e, current):
    user_id = e["user_id"]
    order_no = e['order_no']
    buy_time = e['add_time']
    merit_seller_id = None
    order_type = None
    # 绑定的销售
    # 当时是否绑定销售
    r = DbUtil.select_one("SELECT own_user_id FROM t_own_user_change_log WHERE user_id=%s AND "
                          "add_time <=%s AND type=0 ORDER BY id DESC LIMIT 1;", (user_id, buy_time))
    if r is None:
        # 某些老客户,没有进行过换绑操作无记录
        r = DbUtil.select_one(
            "SELECT own_user_id,customer_id FROM t_seller_user WHERE user_id=%s AND add_time <=%s  LIMIT 1 ",
            (user_id, buy_time))
    if r is None or r['own_user_id'] is None:
        return None
    if order_no is not None:
        order = DbUtil.select_one("SELECT order_type FROM t_order WHERE order_no=%s", order_no)
        order_type = order["order_type"]
    seller_id = r['own_user_id']
    customer_id = r["customer_id"] if hasattr(r, "customer_id") else None
    # 标记绩效归属
    merit_type_result = merit_type_order(e)
    if merit_type_result is None:
        return None
    d = {
        "user_id": user_id,
        "order_no": e["order_no"],
        "order_type": order_type,
        "cost_money": e["money"],
        "seller_id": seller_id,
        "customer_id": customer_id,
        "merit_seller_id": merit_seller_id,
        "type": merit_type_result,
        "remark": e["account_log_no"],
        "add_time": buy_time

    }
    return d


def consist_sql(insert_data):
    start_time = time.time()
    i = 0
    if len(insert_data) > 0:
        sql = "INSERT INTO t_performance (user_id,order_no,order_type," \
              "cost_money,seller_id," \
              "customer_id,merit_seller_id,type,remark,add_time) VALUES"
        sql_data = ""
        for p_result in insert_data:
            e = p_result.get()
            if e is None:
                continue
                print(str(++i))
            sql_data += (",(%s,'%s',%s,%s,%s,%s,%s,%s,'%s','%s')" % (
                e["user_id"], e["order_no"], e["order_type"], e["cost_money"],
                e["seller_id"], e["customer_id"], e["merit_seller_id"],
                e["type"],
                e["remark"], e["add_time"]))
        sql += sql_data.replace(",", "", 1).replace("None", "null") + ";"
        with open(r"D:\account_log_merit_log.txt", "w") as f:
            f.write(sql)
        f.close()
    ent_time = time.time()

    print("本次执行花费%s秒" % ((ent_time - start_time) / 1000))
    print(str(sql))


if __name__ == "__main__":
    # 获取所有订单
    result = DbUtil.select("SELECT id,account_log_no,order_no,cost_type,user_id,money,add_time"
                           " FROM t_account_log"
                           " FORCE INDEX (ind_add_time) "
                           "WHERE cost_type IN(2,3,5,9) AND "
                           "add_time BETWEEN '2020-07-31' AND '2020-08-12';")

    dt = datetime.datetime.now()
    current = dt.strftime("%Y-%m-%d %H:%M:%S")
    print(f"当前时间:{current},长度:{len(result)}")
    pool = Pool(10)
    pool_result = []
    k = 0
    for data in result:
        result = pool.apply_async(operate_merit, (data, current))
        pool_result.append(result)
        if k % 2000 == 0:
            print(str(data))
        ++k
    pool.close()
    pool.join()
    consist_sql(pool_result)
    DbUtil.close()
