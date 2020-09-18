# coding=utf-8
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


def consist_sql(resultArr):
    sql = "UPDATE t_province_city SET"
    sql_m = " area_code=CASE id "
    ids = []
    for e in resultArr:
        id_ = e['id']
        ids.append(str(id_))
        sql_m += f" WHEN {id_} THEN {e['area_code']}"
    sql_m += " END "
    sql += sql_m + " WHERE id IN(" + (','.join(ids)) + ");"
    with open(r"D:\city_area_code.txt", "w") as f:
        f.write(sql)
    f.close()
    print("本次执行花费10秒")
    print(str(sql))


if __name__ == "__main__":
    cityArea = []
    citys = []
    select = DbUtil.select("SELECT id,name FROM t_province_city")
    d = {}
    for e in select:
        d[e['name']] = e["id"]

    city_key = d.keys()
    with open(r"C:\Users\Neon\Downloads\mobiles.sql", "r", encoding="utf-8") as f:
        loop = True
        while loop:
            lines = f.readline()
            if not lines:
                break
                pass
            if lines.find("values") < 0:
                continue
            split = lines.replace(r"'", "").split("values")
            lines = split[1]
            split = lines.split(",")

            i = 0
            while i < len(split) - 1:
                try:
                    city = split[i + 3]
                    area_code = split[i + 4]
                    i = i + 7
                    if city in citys:
                        continue
                    citys.append(city)
                except  Exception as e:
                    print(e)
                    print(lines)
                    loop = False
                    break
                city_id = None
                if city in city_key:
                    city_id = d[city]
                else:
                    city = city + "市"
                if city in city_key:
                    city_id = d[city]
                else:
                    continue

                b = {
                    "city": city,
                    "id": city_id,
                    "area_code": area_code
                }
                cityArea.append(b)
                print(b)
    f.close()
    consist_sql(cityArea)
