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
        ",add_time  FROM t_car_market A INNER JOIN t_user B ON B.car_market_id=A.id WHERE A.pid is not null limit 10;")
    for i, e in enumerate(result, 1):
        user_id = e.pop("userId")
        e["extend"] = user_id
        if e["address"] is None:
            e["address"] = "未知"
    sql = ConsistSql.batch_insert("t_market_shop", result)
    FileUtil.write(f"D:\\market_shop-.sql", sql)
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


# 原有表中的 城市-市场
# 客户匹配表中的 城市-市场
# 同一个城市 市场一致的,不一致的  不一致的找相似的
def market():
    re_compile = re.compile("市场|二手车|交易")
    # 市场总数量
    market = """城市	市场名称
深圳	深圳市桃园二手车
延安	车时代二手车市场
濮阳	濮阳花乡二手车市场
宁波	宁波中基二手车市场
深圳	深圳市建达成二手车
哈尔滨	国际汽车城
临沂	临沂远通二手车交易市场
石家庄	瑞诚二手车市场
深圳	深圳市华尧运二手车
东莞	东莞弘驿二手车市场
长沙	弘高车世界
成都	成都宏盟二手车新市场
重庆	汽博二手车交易市场
大连	后盐车市
洛阳	骅士二手车市场
佛山	佛山宝发二手车市场
东莞	东莞联合二手车市场
东莞	东莞车乐园二手车市场
东莞	东莞弘达二手车市场
东莞	东莞东桥二手车市场
东莞	东莞粤威二手车市场
辽阳	辽阳天合二手车市场
辽阳	辽阳颢华二手车市场
大同	大同阳光车城二手车市场
福州	福州吉众志成二手车市场
沈阳	辽众沈二手车市场
滁州	新概念二手车市场（滁州市顺达汽车贸易有限公司）
许昌	许昌M8二手车市场
宁波	"宁波国际汽车城
（宁波骏元汽车服务有限公司）"
赣州	赣州万众嘉车二手车交易市场有限公司
长沙	湖南潇珑汽车销售服务有限公司
苏州	博豪车世界
长春	长春华港二手车市场
石家庄	西三庄二手车旧市场
运城	鑫义达二手车市场
郑州	河南北翼仟城科技有限公司（中博市场）
天津	运达二手车交易市场
天津	天津市辰丰二手车市场管理有限公司
武汉	一号车市
西安	卡猫二手车市场
沈阳	昆山路二手车市场
沈阳	沈阳亨通二手车市场
烟台	烟台奥托玛特
苏州	苏州瑞尚二手车市场
武汉	武汉竹叶山二手车市场
东莞	东莞粤晟二手车市场
珠海	珠海强利名车二手车市场
南宁	南宁新畅行二手车市场
太原	太原九润二手车市场
北京	亚运村二手车市场
成都	西新广和二手车市场
岳阳	岳阳车世界二手车市场
苏州	
合肥	南翔智慧汽车城
太原	鸿升二手车市场
贵阳	合朋二手车市场
杭州	海外海（杭州）汽车城有限公司
成都	锦和二手车市场
靖江	手拉手汽车小镇
合肥	汽车之家诚信展厅
宁波	姚江市场（宁波大城小辰二手车经纪有限公司、收吧一期）
天津	天津市辰丰二手车市场
福州	钛阳二手车市场
达州	吉鑫二手车市场
深圳	南山区盈百纳二手车
	盛京二手车交易市场
深圳	一通旗舰店
深圳	七通
深圳	深圳市罗湖TT二手车
大连	云达车市
大连	亿丰车市
东莞	粤盈市场
东莞	粤盛市场
东莞	佳能市场
东莞	和鑫行
东莞	汇德丰
哈尔滨	二手车市场
长春	凯旋二手车市场
天津	空港二手车交易市场
天津	广物二手车交易市场
天津	王兰庄二手车交易市场
天津	宝坻二手车交易市场
沧州	鼎奕二手车交易市场
沧州	沧州旧机动车老市场
唐山	吉祥二手车市场
唐山	兴瑭二手车市场
秦皇岛	秦皇岛机动车交易市场
沈阳	塔湾二手车市场
沈阳	北二路汽车街
沈阳	金宝台二手车市场
沈阳	优车湃二手车市场
赤峰	利丰二手车市场
大同	福海二手车交易市场
大同	旧机动车交易市场
青岛	苏尚万车
青岛	陆海二手车交易市场
青岛	君泰二手车市场
青岛	富达二手车市场
青岛	合创二手车市场
青岛	308二手车市场
临沂	兰田
临沂	华东
临沂	河东远通
潍坊	方宜市场
潍坊	德嘉市场
潍坊	金宝市场
潍坊	广潍市场
潍坊	泰华车港市场
潍坊	帅天市场
潍坊	联运车城
烟台	烟台港通二手车交易
烟台	烟台汽车城
石家庄	西三庄二手车新市场
石家庄	花乡二手车市场
邯郸	华信二手车市场
济南	蓝翔路机动车交易中心
济南	泺口二手车市场
济南	济西二手车交易市场
济南	济东二手车交易市场
洛阳	惠通二手车市场
洛阳	林安二手车市场
洛阳	源信二手车市场
运城	金慧二手车市场
徐州	银地二手车交易市场
徐州	徐州淮海汽车城
洛阳	心达二手车市场
洛阳	永信二手车市场
洛阳	富祥二手车市场
烟台	百大市场
南京	大公二手车市场
南京	天诚二手车市场
南京	汽车小镇万璟二手车市场
南京	中驰二手车市场
无锡	神州
无锡	江苏东方二手车交易市场
无锡	阿里市场
呼和浩特	亿丰旧机动车交易市场
呼和浩特	庆瑞二手车市场
呼和浩特	庆鼎二手车市场
呼和浩特	同创汽车城
呼和浩特	车虎二手车市场
呼和浩特	思必达二手车交易市场
呼和浩特	八大二手车交易市场
呼和浩特	思博翔二手车市场
上海	旧机动车交易市场
上海	武宁路二手车交易市场
上海	沪南公路二手车交易市场
上海	二手车交易中心
上海	莘庄旧机动车交易市场
合肥	风之星二手车市场
合肥	省直二手车市场
合肥	周谷堆汽车城
合肥	世联二手车城
合肥	恒信汽车城
合肥	十里庙二手车市场
合肥	亿捷二手车市场
常州	常武
常州	武进
包头	航茂
包头	大麦
包头	佳通
包头	惠友
包头	恒通
苏州	中泰华二手车市场
杭州	萧山新世纪市场
杭州	萧山旧机动车市场
宁波	世纪汽车城
宁波	途众市场
宁波	海达，公运交易市场
宁波	收吧二手车市场（二期）
宁波	收吧二手车市场（四期）、物产远通
福州	中联车网
福州	晋安区
福州	台江区
福州	海峡二手车交易市场
汕头	诚意汽车城
汕头	泰龙汽车城
温州	车立方
温州	汽车城
温州	广纳五金
台州	方林二手车市场
台州	新安南街市场
台州	椒江恒通市场
台州	椒江德意之星市场
台州	临海台运市场
武汉	人民汽车城
武汉	升官渡二手车市场
武汉	长丰爱之家二手车市场
武汉	汉口北二手车市场
荆门	万里二手车交易市场
荆门	格林美二手车市场
郑州	北所
郑州	香山路二手车城
郑州	好车港
长沙	中南二手车市场
长沙	俊和二手车市场
长沙	河西市场
株洲	宜天二手车市场
佛山	俊丰堡
佛山	壹邦汽车城
佛山	新锋景
佛山	峰景
佛山	旧峰景
佛山	广进旧机动车交易市场
佛山	新协力
佛山	锦源
佛山	高明旧机
佛山	亿强
佛山	顺德旧机动
佛山	西樵
中山	大岭二手车市场
中山	长江北旧机动车交易市场
中山	鸿源机动车交易市场
中山	华骏二手车市场
中山	聚源二手车交易市场
惠州	车世界
惠州	焦点市场
广州	广骏主场
广州	广骏分场
广州	宝利捷主场
广州	宝利捷分场
广州	天羽二手车市场
广州	永泰二手车市场
广州	花都空港二手车城
深圳	深圳市新大运二手车
深圳	深圳市远望二手车
深圳	深圳市龙珠二手车
深圳	深圳市龙达二手车
深圳	深圳市大昌行二手车
深圳	深圳市明雅二手车
深圳	深圳市百威年二手车
东莞	莞一
东莞	汇得丰
东莞	新华利
东莞	永一佳
东莞	张博名车
东莞	福瑞通
珠海	众大利二手车市场
珠海	华茂二手车市场
珠海	骏途二手车市场
珠海	中润二手车市场
南宁	广隆二手车市场
南宁	新广隆二手车市场
南宁	安吉
南宁	老安吉
南宁	东盟二手车市场
南宁	江南国际二手车市场
南宁	吉运二手车市场
南宁	金硕二手车市场
南宁	金桥二手车市场
南宁	沙井市场
西宁	小峡二手车市场
西宁	欧博二手车市场
西宁	E车市场
西宁	海湖车城
庆阳	陇秀车城
庆阳	高祖广场
乌鲁木齐	通汇二手车市场
乌鲁木齐	赛博特二手车市场
乌鲁木齐	粤和二手车市场
武威	武威二手车交易市场
武威	华人二手车交易市场
太原	万国二手车市场
西安	鱼化
西安	石桥汽车城
西安	华亿二手车
西安	公诚
西安	车超市
西安	吉发
西安	诚成
西安	恒通
西安	峰泰
银川	功达二手车市场
银川	西夏区二手车市场
银川	金三角二手车市场
北京	花乡旧机动车市场
北京	新发地二手车市场
北京	东方基业汽车城
北京	顺潮东二手车市场
重庆	四公里二手车市场
重庆	六公里二手车交易市场
重庆	八公里西部国际汽车城
重庆	九公里二手车交易市场
贵阳	清镇济辉汽车城
贵阳	车之都市场
贵阳	万众国际二手车市场
成都	宸宇二手车市场
成都	西部汽车城
成都	易车时代
成都	万盛二手车市场
成都	宝捷二手车市场
昆明	新车行天
昆明	锦大市场
昆明	凯旋利市场
昆明	省二手车市场
昆明	老车行天下
苏州	富格尔名车广场
保定	保定腾运市场
泉州	海西汽配城
泉州	海天汽车城二手车市场
泉州	优适得二手车市场
泉州	全球通汽车交易市场
泉州	国联二手车交易市场
泉州	华创二手车广场
宁德	东方红市场
宁德	枋湖路二手车市场
"""
    market_list = market.split("\n")
    read = ExcelUtil.read(
        r"C:\Users\Neon\Documents\WeChat Files\wxid_8468304683212\FileStorage\File\2020-09\车商 明细.xlsx", "车商明细")
    market_name_arr = []
    # 客户市场关系中的 所有 城市-市场
    for e in read:
        if e is None or e[3] is None or e[2] is None: continue
        e_ = {"name": e[3], "city": e[2]}
        e_t = {"name": e[3], "city": e[2], "address": e[4]}
        if e is not None and e_ not in market_name_arr:
            market_name_arr.append(e_)
            market_name_arr.append(e_t)
    result_market = {}
    for m in market_name_arr:
        user_market_name = m["name"]
        user_market_city = m["city"]
        for market_city_8_name in market_list:
            if market_city_8_name is None:
                continue
            city_market = market_city_8_name.split("\t")
            if len(city_market) < 2:
                continue
            city_name = city_market[0]
            market_name = city_market[1]
            if city_name is None or market_name is None: continue
            if user_market_city != str(city_name):
                continue
            rate = get_equal_rate(re_compile.sub("", str(market_name)), re_compile.sub("", str(user_market_name)))
            rate_ = m.get("rate", 0)
            if rate_ < rate:
                m["rate"] = round(rate, 2)
                m["match"] = market_name
    market_name_arr.sort(key=lambda k: -1 if k["name"] == "场外" else k.get("rate", 0), reverse=True)
    ExcelUtil.write("D:\车商-市场.xlsx", market_name_arr)


def market_address():
    re_compile = re.compile("市场|二手|车|交易|世界|行|")
    # 市场总数量
    market = """城市	市场名称
深圳	深圳市桃园二手车
延安	车时代二手车市场
濮阳	濮阳花乡二手车市场
宁波	宁波中基二手车市场
深圳	深圳市建达成二手车
哈尔滨	国际汽车城
临沂	临沂远通二手车交易市场
石家庄	瑞诚二手车市场
深圳	深圳市华尧运二手车
东莞	东莞弘驿二手车市场
长沙	弘高车世界
成都	成都宏盟二手车新市场
重庆	汽博二手车交易市场
大连	后盐车市
洛阳	骅士二手车市场
佛山	佛山宝发二手车市场
东莞	东莞联合二手车市场
东莞	东莞车乐园二手车市场
东莞	东莞弘达二手车市场
东莞	东莞东桥二手车市场
东莞	东莞粤威二手车市场
辽阳	辽阳天合二手车市场
辽阳	辽阳颢华二手车市场
大同	大同阳光车城二手车市场
福州	福州吉众志成二手车市场
沈阳	辽众沈二手车市场
滁州	新概念二手车市场（滁州市顺达汽车贸易有限公司）
许昌	许昌M8二手车市场
宁波	"宁波国际汽车城
（宁波骏元汽车服务有限公司）"
赣州	赣州万众嘉车二手车交易市场有限公司
长沙	湖南潇珑汽车销售服务有限公司
苏州	博豪车世界
长春	长春华港二手车市场
石家庄	西三庄二手车旧市场
运城	鑫义达二手车市场
郑州	河南北翼仟城科技有限公司（中博市场）
天津	运达二手车交易市场
天津	天津市辰丰二手车市场管理有限公司
武汉	一号车市
西安	卡猫二手车市场
沈阳	昆山路二手车市场
沈阳	沈阳亨通二手车市场
烟台	烟台奥托玛特
苏州	苏州瑞尚二手车市场
武汉	武汉竹叶山二手车市场
东莞	东莞粤晟二手车市场
珠海	珠海强利名车二手车市场
南宁	南宁新畅行二手车市场
太原	太原九润二手车市场
北京	亚运村二手车市场
成都	西新广和二手车市场
岳阳	岳阳车世界二手车市场
苏州	
合肥	南翔智慧汽车城
太原	鸿升二手车市场
贵阳	合朋二手车市场
杭州	海外海（杭州）汽车城有限公司
成都	锦和二手车市场
靖江	手拉手汽车小镇
合肥	汽车之家诚信展厅
宁波	姚江市场（宁波大城小辰二手车经纪有限公司、收吧一期）
天津	天津市辰丰二手车市场
福州	钛阳二手车市场
达州	吉鑫二手车市场
深圳	南山区盈百纳二手车
	盛京二手车交易市场
深圳	一通旗舰店
深圳	七通
深圳	深圳市罗湖TT二手车
大连	云达车市
大连	亿丰车市
东莞	粤盈市场
东莞	粤盛市场
东莞	佳能市场
东莞	和鑫行
东莞	汇德丰
哈尔滨	二手车市场
长春	凯旋二手车市场
天津	空港二手车交易市场
天津	广物二手车交易市场
天津	王兰庄二手车交易市场
天津	宝坻二手车交易市场
沧州	鼎奕二手车交易市场
沧州	沧州旧机动车老市场
唐山	吉祥二手车市场
唐山	兴瑭二手车市场
秦皇岛	秦皇岛机动车交易市场
沈阳	塔湾二手车市场
沈阳	北二路汽车街
沈阳	金宝台二手车市场
沈阳	优车湃二手车市场
赤峰	利丰二手车市场
大同	福海二手车交易市场
大同	旧机动车交易市场
青岛	苏尚万车
青岛	陆海二手车交易市场
青岛	君泰二手车市场
青岛	富达二手车市场
青岛	合创二手车市场
青岛	308二手车市场
临沂	兰田
临沂	华东
临沂	河东远通
潍坊	方宜市场
潍坊	德嘉市场
潍坊	金宝市场
潍坊	广潍市场
潍坊	泰华车港市场
潍坊	帅天市场
潍坊	联运车城
烟台	烟台港通二手车交易
烟台	烟台汽车城
石家庄	西三庄二手车新市场
石家庄	花乡二手车市场
邯郸	华信二手车市场
济南	蓝翔路机动车交易中心
济南	泺口二手车市场
济南	济西二手车交易市场
济南	济东二手车交易市场
洛阳	惠通二手车市场
洛阳	林安二手车市场
洛阳	源信二手车市场
运城	金慧二手车市场
徐州	银地二手车交易市场
徐州	徐州淮海汽车城
洛阳	心达二手车市场
洛阳	永信二手车市场
洛阳	富祥二手车市场
烟台	百大市场
南京	大公二手车市场
南京	天诚二手车市场
南京	汽车小镇万璟二手车市场
南京	中驰二手车市场
无锡	神州
无锡	江苏东方二手车交易市场
无锡	阿里市场
呼和浩特	亿丰旧机动车交易市场
呼和浩特	庆瑞二手车市场
呼和浩特	庆鼎二手车市场
呼和浩特	同创汽车城
呼和浩特	车虎二手车市场
呼和浩特	思必达二手车交易市场
呼和浩特	八大二手车交易市场
呼和浩特	思博翔二手车市场
上海	旧机动车交易市场
上海	武宁路二手车交易市场
上海	沪南公路二手车交易市场
上海	二手车交易中心
上海	莘庄旧机动车交易市场
合肥	风之星二手车市场
合肥	省直二手车市场
合肥	周谷堆汽车城
合肥	世联二手车城
合肥	恒信汽车城
合肥	十里庙二手车市场
合肥	亿捷二手车市场
常州	常武
常州	武进
包头	航茂
包头	大麦
包头	佳通
包头	惠友
包头	恒通
苏州	中泰华二手车市场
杭州	萧山新世纪市场
杭州	萧山旧机动车市场
宁波	世纪汽车城
宁波	途众市场
宁波	海达，公运交易市场
宁波	收吧二手车市场（二期）
宁波	收吧二手车市场（四期）、物产远通
福州	中联车网
福州	晋安区
福州	台江区
福州	海峡二手车交易市场
汕头	诚意汽车城
汕头	泰龙汽车城
温州	车立方
温州	汽车城
温州	广纳五金
台州	方林二手车市场
台州	新安南街市场
台州	椒江恒通市场
台州	椒江德意之星市场
台州	临海台运市场
武汉	人民汽车城
武汉	升官渡二手车市场
武汉	长丰爱之家二手车市场
武汉	汉口北二手车市场
荆门	万里二手车交易市场
荆门	格林美二手车市场
郑州	北所
郑州	香山路二手车城
郑州	好车港
长沙	中南二手车市场
长沙	俊和二手车市场
长沙	河西市场
株洲	宜天二手车市场
佛山	俊丰堡
佛山	壹邦汽车城
佛山	新锋景
佛山	峰景
佛山	旧峰景
佛山	广进旧机动车交易市场
佛山	新协力
佛山	锦源
佛山	高明旧机
佛山	亿强
佛山	顺德旧机动
佛山	西樵
中山	大岭二手车市场
中山	长江北旧机动车交易市场
中山	鸿源机动车交易市场
中山	华骏二手车市场
中山	聚源二手车交易市场
惠州	车世界
惠州	焦点市场
广州	广骏主场
广州	广骏分场
广州	宝利捷主场
广州	宝利捷分场
广州	天羽二手车市场
广州	永泰二手车市场
广州	花都空港二手车城
深圳	深圳市新大运二手车
深圳	深圳市远望二手车
深圳	深圳市龙珠二手车
深圳	深圳市龙达二手车
深圳	深圳市大昌行二手车
深圳	深圳市明雅二手车
深圳	深圳市百威年二手车
东莞	莞一
东莞	汇得丰
东莞	新华利
东莞	永一佳
东莞	张博名车
东莞	福瑞通
珠海	众大利二手车市场
珠海	华茂二手车市场
珠海	骏途二手车市场
珠海	中润二手车市场
南宁	广隆二手车市场
南宁	新广隆二手车市场
南宁	安吉
南宁	老安吉
南宁	东盟二手车市场
南宁	江南国际二手车市场
南宁	吉运二手车市场
南宁	金硕二手车市场
南宁	金桥二手车市场
南宁	沙井市场
西宁	小峡二手车市场
西宁	欧博二手车市场
西宁	E车市场
西宁	海湖车城
庆阳	陇秀车城
庆阳	高祖广场
乌鲁木齐	通汇二手车市场
乌鲁木齐	赛博特二手车市场
乌鲁木齐	粤和二手车市场
武威	武威二手车交易市场
武威	华人二手车交易市场
太原	万国二手车市场
西安	鱼化
西安	石桥汽车城
西安	华亿二手车
西安	公诚
西安	车超市
西安	吉发
西安	诚成
西安	恒通
西安	峰泰
银川	功达二手车市场
银川	西夏区二手车市场
银川	金三角二手车市场
北京	花乡旧机动车市场
北京	新发地二手车市场
北京	东方基业汽车城
北京	顺潮东二手车市场
重庆	四公里二手车市场
重庆	六公里二手车交易市场
重庆	八公里西部国际汽车城
重庆	九公里二手车交易市场
贵阳	清镇济辉汽车城
贵阳	车之都市场
贵阳	万众国际二手车市场
成都	宸宇二手车市场
成都	西部汽车城
成都	易车时代
成都	万盛二手车市场
成都	宝捷二手车市场
昆明	新车行天
昆明	锦大市场
昆明	凯旋利市场
昆明	省二手车市场
昆明	老车行天下
苏州	富格尔名车广场
保定	保定腾运市场
泉州	海西汽配城
泉州	海天汽车城二手车市场
泉州	优适得二手车市场
泉州	全球通汽车交易市场
泉州	国联二手车交易市场
泉州	华创二手车广场
宁德	东方红市场
宁德	枋湖路二手车市场
"""
    market_list = market.split("\n")
    select_result = db.select(
        "SELECT A.name,A.address,tpc.name city FROM t_car_market A INNER JOIN t_province_city tpc on A.city_id = tpc.id "
        "WHERE A.pid is null or  A.pid=0;")
    market_name_arr = []
    # 客户市场关系中的 所有 城市-市场
    market_dict_list = []
    for market_detail in market_list:
        if market_detail is None:
            continue
        city_market = market_detail.split("\t")
        if len(city_market) < 2:
            continue
        city_name = city_market[0]
        market_name = city_market[1]
        market_dict_list.append({"city": city_name, "market": market_name})

    for m in market_dict_list:
        user_market_name = m.get("market")
        user_market_city = m.get("city")
        for market_detail in select_result:
            market_name = market_detail.get("name")
            city_name = market_detail.get("city").replace("市", "")
            if city_name != user_market_city or market_name == '场外':
                continue
            rate = get_equal_rate(re_compile.sub("", str(market_name)),
                                  re_compile.sub("", str(user_market_name)))
            rate_ = m.get("rate", 0)
            if rate_ < rate:
                m["rate"] = round(rate, 2)
                m["address"] = market_detail.get("address")
    market_dict_list.sort(key=lambda k: -1 if k.get("market") == "场外" else k.get("rate", 0), reverse=True)
    ExcelUtil.write("D:\车商-市场_地址.xlsx", market_dict_list)


"""
1.店铺信息导出,生成sql,插入到market_shop
2.更新user_detail中的market_shop_id
3.删除所有市场信息,插入新的市场
4.更新用户市场绑定关系
5.更新market_shop中的car_market_id
"""

if __name__ == "__main__":
    market_address()
# market_ins()
# update_user_detail_market_shop()
