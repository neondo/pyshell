import datetime
import logging
import math

import pymysql
import xlrd
from pandas import DataFrame
from functools import partial
import logging
from multiprocessing import Pool, Value, Array, Manager, Lock

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='util.log',
                    filemode='w')
lock = Lock()


class DbUtil:
    def __init__(self, name):
        if name == "m":
            self.conn = pymysql.connect(host="172.17.202.101", port=3308, user="read_only", password="kjbcZtQ1BVv76kpC",
                                        database="db_www", charset="utf8")
        elif name == "t":
            self.conn = pymysql.connect(host="172.17.202.121", port=58885, user="develop", password="chaboshitest",
                                        database="db_www", charset="utf8")
        else:
            raise Exception('找不到对应数据库!')
        self.cur = self.conn.cursor(pymysql.cursors.DictCursor)

    def select(self, sql, arg=None):
        self.cur.execute(sql, arg)
        return self.cur.fetchall()

    def select_one(self, sql, arg=None):
        self.cur.execute(sql, arg)
        return self.cur.fetchone()

    def close(self):
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None:
            self.conn.close()


class ConsistSql:

    @staticmethod
    def batch_update(table_name, datalist=[{}], _id=None):
        data_len = len(datalist)
        print("consist update sql start,len:%s" % data_len)
        sql = f"UPDATE {table_name} SET"
        pool_size = 10
        _id = Manager().list()
        pool = Pool(pool_size)
        size = math.ceil(float(data_len) / pool_size)
        data_arr = []
        for i in range(pool_size):
            data_arr.append(datalist[i * size:(i + 1) * size])
        p = partial(ConsistSql.method_name, _id=_id)
        result = pool.map_async(p, data_arr)
        pool.close()
        pool.join()
        values_result = {}
        result = result.get()
        for result_val in result:
            for (k, v) in result_val.items():
                val = values_result[k] if k in values_result else f" `{k}`=CASE id"
                val += v
                values_result[k] = val
        for (k, v) in values_result.items():
            sql += f"{v} END,"
            sql = sql[0:-1]
        sql += f" END WHERE id IN({(','.join(_id))});"
        print("consist update sql end:%s" % sql)
        return sql

    @staticmethod
    def batch_update_single(table_name, datalist=[{}]):
        data_len = len(datalist)
        if data_len == 0: return
        _id = []
        print("consist update sql start,len:%s" % data_len)
        sql = f"UPDATE {table_name} SET"
        result_val = ConsistSql.method_name(datalist, _id)
        for (k, v) in result_val.items():
            sql += f" `{k}`=CASE id {v} END,"
        sql = sql[0:-1]
        sql += f"  WHERE id IN({(','.join(_id))});"
        print("consist update sql end:%s" % sql)
        return sql

    @staticmethod
    def method_name(data_list, _id):
        values = {}
        for e in data_list:
            e_id = e.pop("id")
            _id.append(str(e_id))
            for (k, v) in e.items():
                val = values[k] if k in values else ""
                v = f"'{v}'" if not_null(v) else k
                val += f" WHEN {str(e_id)} THEN {v}"
                values[k] = val
        return values

    @staticmethod
    def batch_insert(table_name, datalist=[{}]):
        print("consist insert sql start,len:%s" % len(datalist))
        keys = datalist[0].keys()
        sql = f"INSERT INTO {table_name} ({','.join(str(i) for i in keys)})VALUES"
        for e in datalist:
            sql += f"""({','.join("'" + str(x) + "'" for x in e.values())}),"""
        sql = sql[0:-1].replace(f'\'None\'', 'null')
        print("consist insert sql end:%s" % sql)
        return sql + ";"


class ExcelUtil:

    @staticmethod
    def read(path_file, sheet_name=None, start=0):
        if sheet_name is None:
            sheet_name = "Sheet1"
        data = []
        with xlrd.open_workbook(path_file, ) as f:
            table = f.sheet_by_name(sheet_name)
            for i in range(start, table.nrows):
                data.append(table.row_values(i))
        return data

    @staticmethod
    def write(path_file, data=[[]]):
        frame = DataFrame(data)
        frame.to_excel(path_file, index=False)


class FileUtil:

    @staticmethod
    def read(path_file, sheet_name=None):
        arr = []
        with open(path_file, "r") as f:
            while True:
                lines = f.readline()
                if not lines:
                    break
                    pass
                arr.append(lines.strip())
        return arr

    @staticmethod
    def write(path_file, a):
        with open(path_file, "w", encoding='utf-8') as f:
            if type(a) == str:
                f.write(a)
            elif type(a) == list:
                for i in a:
                    f.write(a + "\n")


def int_v(val):
    return int(val) if type(val) is float and val % 1 == 0 else val


def not_null(val):
    return val is not None and str(val) != ""


def trim(val):
    return val.strip() if type(val) == str else val