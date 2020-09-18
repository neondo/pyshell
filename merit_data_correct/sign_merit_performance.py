# coding=utf-8
from multiprocessing import Pool
import datetime
from cn.util.util import DbUtil


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


dt = datetime.datetime.now()
current = dt.strftime("%Y-%m-%d %H:%M:%S")

resultArr = []  # 需要更新的数据


def operator_merit(e, current):
    user_id = e["user_id"]
    # 是否是定制估价-实车检测
    if e['order_type'] == 19:
        r_valuation = DbUtil.select_one(
            "SELECT detection_orderno FROM t_valuation_info_customed WHERE order_no=%s LIMIT 1;",
            e['order_no'])
        if r_valuation is None or r_valuation['detection_orderno'] is None:
            return None
    # 当时是否绑定销售
    add_time = e['add_time']
    r = DbUtil.select_one(
        "SELECT own_user_id FROM t_own_user_change_log "
        "WHERE user_id=%s AND type=0 AND add_time <=%s ORDER BY id DESC LIMIT 1 ",
        (user_id, add_time))
    if r is None:
        # 某些老客户,没有进行过换绑操作无记录
        r = DbUtil.select_one(
            "SELECT own_user_id FROM t_seller_user WHERE user_id=%s AND add_time <=%s  LIMIT 1 ",
            (user_id, add_time))
        # 当时有绑定记录的客户继续校验
    if r is None or r["own_user_id"] is None:
        return None
    sign_result = validity_sign(user_id, add_time)  # 判定是否可标记
    if sign_result:
        r_p = DbUtil.select_one("SELECT id FROM t_performance WHERE order_no=%s", e['order_no'])
        if r_p is None:
            return None
        update_data = {
            "merit_seller_id": r['own_user_id'],
            "id": r_p['id'],
            "remark": current
        }
        print("时间:%s" % add_time)
        return update_data
    return None


def consist_sql(resultArr):
    sql = "UPDATE t_performance SET"
    sql_m = " merit_seller_id=CASE id "
    ids = []
    for D in resultArr:
        e = D.get()
        if e is None:
            continue
        id_ = e['id']
        ids.append(str(id_))
        sql_m += f" WHEN {id_} THEN {e['merit_seller_id']}"
    sql_m += " END "
    sql += sql_m + " WHERE id IN(" + (','.join(ids)) + ");"
    with open(r"D:\account_log_merit_update.txt", "w") as f:
        f.write(sql)
    f.close()
    print("本次执行花费10秒")
    print(str(sql))


if __name__ == "__main__":
    # 获取所有订单
    result = DbUtil.select("SELECT order_no,order_type,user_id,add_time FROM t_performance "
                           "WHERE add_time BETWEEN '2020-07-31' AND '2020-08-13' "
                           "AND order_type IN(5, 6, 10, 19) AND type=0 "
                           "AND merit_seller_id IS NULL;")

    dt = datetime.datetime.now()
    current = dt.strftime("%Y-%m-%d %H:%M:%S")
    print("当前时间:%s,len:%s" % (current, len(result)))
    pool = Pool(15)
    pool_result = []
    for data in result:
        result = pool.apply_async(operator_merit, (data, current))
        pool_result.append(result)
    pool.close()
    pool.join()
    consist_sql(pool_result)
