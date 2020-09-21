# coding=utf-8
from multiprocessing import Pool
import datetime
from functools import partial
from cn.util.util import *
import difflib
import re


def get_equal_rate(str1='', str2=''):
    return difflib.SequenceMatcher(None, str1, str2).quick_ratio()


db = DbUtil("m")


def match(d, e):
    if d["id"] == e["id"] or d["city_id"] != e["city_id"]:
        return None
    rate = 0
    rate1 = 0
    try:
        rate = get_equal_rate(e["name"].replace("二手车市场", ""), d["name"].replace("二手车市场", ""))
        rate1 = get_equal_rate(e["address"], d["address"])
    except Exception as e:
        print(e)

    if rate > 0.8 or rate1 > 0.8:
        fix = {"id": e["id"], "id_du": d["id"], "name": e["name"], "name_dun": d["name"], "address": e["address"],
               "address_dun": d["address"]}
        return fix


def market_ins():
    # 获取所有订单
    result = db.select(
        "SELECT A.name,A.address,A.pid car_market_id,parking_space_num carport,B.id userId "
        ",add_time  FROM t_car_market A INNER JOIN t_user B ON B.car_market_id=A.id WHERE A.pid is not null;")
    for i, e in enumerate(result, 1):
        user_id = e.pop("userId")
        e["id"] = i + 355
        e["extend"] = user_id
        if e["address"] is None:
            e["address"] = "未知"
    sql = ConsistSql.batch_insert("t_market_shop", result)
    FileUtil.write(f"D:\\market_shop_insert.sql", sql)
    dt = datetime.datetime.now()
    current = dt.strftime("%Y-%m-%d %H:%M:%S")
    print("当前时间:%s,len:%s" % (current, len(result)))


def update_user_detail_market_shop():
    # 获取所有订单
    result = db.select(
        "SELECT C.id  FROM t_car_market A INNER JOIN  t_user B ON B.car_market_id=A.id "
        "INNER JOIN  t_user_detail C ON B.id=C.user_id WHERE A.pid is not null;")
    for i, e in enumerate(result, 1):
        e["market_shop_id"] = i
    sql = ConsistSql.batch_update_single("t_user_detail", result)
    FileUtil.write(f"D:\\user_detail_market_shop_ud.sql", sql)
    dt = datetime.datetime.now()
    current = dt.strftime("%Y-%m-%d %H:%M:%S")
    print("当前时间:%s,len:%s" % (current, len(result)))


def market():
    re_compile = re.compile("市场|二手车|交易")
    market = """市场名称
深圳市桃园二手车
车时代二手车市场
濮阳花乡二手车市场
宁波中基二手车市场
深圳市建达成二手车
国际汽车城
临沂远通二手车交易市场
瑞诚二手车市场
深圳市华尧运二手车
东莞弘驿二手车市场
弘高车世界
成都宏盟二手车新市场
汽博二手车交易市场
后盐车市
骅士二手车市场
佛山宝发二手车市场
东莞联合二手车市场
东莞车乐园二手车市场
东莞弘达二手车市场
东莞东桥二手车市场
东莞粤威二手车市场
辽阳天合二手车市场
辽阳颢华二手车市场
大同阳光车城二手车市场
福州吉众志成二手车市场
辽众沈二手车市场
新概念二手车市场（滁州市顺达汽车贸易有限公司）
许昌M8二手车市场
"宁波国际汽车城
（宁波骏元汽车服务有限公司）"
赣州万众嘉车二手车交易市场有限公司
湖南潇珑汽车销售服务有限公司
博豪车世界
长春华港二手车市场
西三庄二手车旧市场
鑫义达二手车市场
河南北翼仟城科技有限公司（中博市场）
运达二手车交易市场
天津市辰丰二手车市场管理有限公司
一号车市
卡猫二手车市场
昆山路二手车市场
沈阳亨通二手车市场
烟台奥托玛特
苏州瑞尚二手车市场
武汉竹叶山二手车市场
东莞粤晟二手车市场
珠海强利名车二手车市场
南宁新畅行二手车市场
太原九润二手车市场
亚运村二手车市场
西新广和二手车市场
岳阳车世界二手车市场

南翔智慧汽车城
鸿升二手车市场
合朋二手车市场
海外海（杭州）汽车城有限公司
锦和二手车市场
手拉手汽车小镇
汽车之家诚信展厅
姚江市场（宁波大城小辰二手车经纪有限公司、收吧一期）
天津市辰丰二手车市场
钛阳二手车市场
吉鑫二手车市场
南山区盈百纳二手车
盛京二手车交易市场
一通旗舰店
七通
深圳市罗湖TT二手车
云达车市
亿丰车市
粤盈市场
粤盛市场
佳能市场
和鑫行
汇德丰
二手车市场
凯旋二手车市场
空港二手车交易市场
广物二手车交易市场
王兰庄二手车交易市场
宝坻二手车交易市场
鼎奕二手车交易市场
沧州旧机动车老市场
吉祥二手车市场
兴瑭二手车市场
秦皇岛机动车交易市场
塔湾二手车市场
北二路汽车街
金宝台二手车市场
优车湃二手车市场
利丰二手车市场
福海二手车交易市场
旧机动车交易市场
苏尚万车
陆海二手车交易市场
君泰二手车市场
富达二手车市场
合创二手车市场
308二手车市场
兰田
华东
河东远通
方宜市场
德嘉市场
金宝市场
广潍市场
泰华车港市场
帅天市场
联运车城
烟台港通二手车交易
烟台汽车城
西三庄二手车新市场
花乡二手车市场
华信二手车市场
蓝翔路机动车交易中心
泺口二手车市场
济西二手车交易市场
济东二手车交易市场
惠通二手车市场
林安二手车市场
源信二手车市场
金慧二手车市场
银地二手车交易市场
徐州淮海汽车城
心达二手车市场
永信二手车市场
富祥二手车市场
百大市场
大公二手车市场
天诚二手车市场
汽车小镇万璟二手车市场
中驰二手车市场
神州
江苏东方二手车交易市场
阿里市场
亿丰旧机动车交易市场
庆瑞二手车市场
庆鼎二手车市场
同创汽车城
车虎二手车市场
思必达二手车交易市场
八大二手车交易市场
思博翔二手车市场
旧机动车交易市场
武宁路二手车交易市场
沪南公路二手车交易市场
二手车交易中心
莘庄旧机动车交易市场
风之星二手车市场
省直二手车市场
周谷堆汽车城
世联二手车城
恒信汽车城
十里庙二手车市场
亿捷二手车市场
常武
武进
航茂
大麦
佳通
惠友
恒通
中泰华二手车市场
萧山新世纪市场
萧山旧机动车市场
世纪汽车城
途众市场
海达，公运交易市场
收吧二手车市场（二期）
收吧二手车市场（四期）、物产远通
中联车网
晋安区
台江区
海峡二手车交易市场
诚意汽车城
泰龙汽车城
车立方
汽车城
广纳五金
方林二手车市场
新安南街市场
椒江恒通市场
椒江德意之星市场
临海台运市场
人民汽车城
升官渡二手车市场
长丰爱之家二手车市场
汉口北二手车市场
万里二手车交易市场
格林美二手车市场
北所
香山路二手车城
好车港
中南二手车市场
俊和二手车市场
河西市场
宜天二手车市场
俊丰堡
壹邦汽车城
新锋景
峰景
旧峰景
广进旧机动车交易市场
新协力
锦源
高明旧机
亿强
顺德旧机动
西樵
大岭二手车市场
长江北旧机动车交易市场
鸿源机动车交易市场
华骏二手车市场
聚源二手车交易市场
车世界
焦点市场
广骏主场
广骏分场
宝利捷主场
宝利捷分场
天羽二手车市场
永泰二手车市场
花都空港二手车城
深圳市新大运二手车
深圳市远望二手车
深圳市龙珠二手车
深圳市龙达二手车
深圳市大昌行二手车
深圳市明雅二手车
深圳市百威年二手车
莞一
汇得丰
新华利
永一佳
张博名车
福瑞通
众大利二手车市场
华茂二手车市场
骏途二手车市场
中润二手车市场
广隆二手车市场
新广隆二手车市场
安吉
老安吉
东盟二手车市场
江南国际二手车市场
吉运二手车市场
金硕二手车市场
金桥二手车市场
沙井市场
小峡二手车市场
欧博二手车市场
E车市场
海湖车城
陇秀车城
高祖广场
通汇二手车市场
赛博特二手车市场
粤和二手车市场
武威二手车交易市场
华人二手车交易市场
万国二手车市场
鱼化
石桥汽车城
华亿二手车
公诚
车超市
吉发
诚成
恒通
峰泰
功达二手车市场
西夏区二手车市场
金三角二手车市场
花乡旧机动车市场
新发地二手车市场
东方基业汽车城
顺潮东二手车市场
四公里二手车市场
六公里二手车交易市场
八公里西部国际汽车城
九公里二手车交易市场
清镇济辉汽车城
车之都市场
万众国际二手车市场
宸宇二手车市场
西部汽车城
易车时代
万盛二手车市场
宝捷二手车市场
新车行天
锦大市场
凯旋利市场
省二手车市场
老车行天下
富格尔名车广场
保定腾运市场
海西汽配城
海天汽车城二手车市场
优适得二手车市场
全球通汽车交易市场
国联二手车交易市场
华创二手车广场
东方红市场
枋湖路二手车市场
""";
    split = market.split("\n")
    result = db.select("SELECT id,name FROM t_car_market where pid is null or pid=0")
    for e in result:
        name_ = e["name"]
        for j in split:
            rate = get_equal_rate(re_compile.sub("", name_), re_compile.sub("", j))
            if 0.7 < rate < 1:
                print(j, name_)


if __name__ == "__main__":
    # market()
    market_ins()
# update_user_detail_market_shop()
