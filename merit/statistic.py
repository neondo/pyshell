from pyecharts import Funnel, Gauge, Graph

attr = ["衬衫", "羊毛衫", "雪纺衫", "裤子", "高跟鞋", "袜子"]
v1 = [5, 20, 36, 56, 78, 100]
v2 = [55, 60, 16, 20, 15, 80]
# 仪表盘
gauge = Gauge("仪表盘")
gauge.add('业务指标', '完成率', 66.66)
gauge.show_config()
gauge.render(path="./statistic.html")
