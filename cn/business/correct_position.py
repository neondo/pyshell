from cn.util.util import *
import os

db = DbUtil("m")

phone_seller_position_id = []


def loop_position_parent(parent_id):
    result = db.select(
        f"SELECT id FROM t_own_position WHERE parent_id IN({','.join(str(x['id']) for x in parent_id)})", )
    if result is not None:
        arr = []
        for i in result:
            arr.append(i)
            phone_seller_position_id.append(i)
        if len(arr) > 0:
            loop_position_parent(arr)


if __name__ == '__main__':
    loop_position_parent([{"id": 812}])
    arr = []
    r = db.select(
        f"SELECT A.id,A.name FROM t_own_user A LEFT JOIN  t_own_user_position B ON B.own_user_id=A.id"
        f" WHERE B.own_position_id IN({','.join(str(x['id']) for x in phone_seller_position_id)})"
        f" AND A.status=0 group by A.id")
    for i in r:
        id_ = i["id"]
        if id_ not in (4, 526, 2447, 3098):
            arr.append(id_)
    r = db.select(
        f"SELECT A.id,A.name FROM t_own_user A LEFT JOIN  t_own_user_position B ON B.own_user_id=A.id"
        f" WHERE B.own_position_id NOT IN({','.join(str(x['id']) for x in phone_seller_position_id)})"
        f" AND A.status=0 AND A.id IN({','.join(str(x) for x in arr)}) group by A.id")
    db.close()
