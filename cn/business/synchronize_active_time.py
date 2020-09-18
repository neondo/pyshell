from cn.util.util import *
from multiprocessing import Pool

db = DbUtil("m")


def witee(start, end):
    result = db.select("SELECT B.id,MAX(A.add_time) last_link_time FROM t_follow_record A INNER JOIN t_cardealer  B "
                       "ON B.id=A.car_dealer_id WHERE A.id >= %d AND A.id<%d GROUP BY A.car_dealer_id" % (start, end))
    return ConsistSql.batch_update_single("t_cardealer", result)


if __name__ == '__main__':
    with open(r"D:\\car_dealer_link_time.txt", "a") as f:
        result = db.select("SELECT B.id,MAX(A.add_time) last_link_time FROM "
                           "t_follow_record A INNER JOIN t_cardealer  B ON B.id=A.car_dealer_id where "
                           "   A.add_time>='2020-03-01' GROUP BY A.id")
        data_len = len(result)
        pool_size = 5
        pool = Pool(pool_size)
        size = math.ceil(float(data_len) / pool_size)
        result_arr = []
        for i in range(pool_size):
            data = result[i * size:(i + 1) * size]
            re = pool.apply_async(ConsistSql.batch_update_single, ("t_cardealer", data))
            result_arr.append(re)
        for e in result_arr:
            sql = e.get()
            if sql is not None:
                f.write(sql)
                f.flush()
        f.close()
        db.close()
