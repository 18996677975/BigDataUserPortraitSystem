import happybase
import pandas as pd
from pyecharts.charts import Line, Scatter, Bar, Radar
import pyecharts.options as opts
from pyecharts.commons.utils import JsCode

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()

"""连接表"""
basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
socialAttributesTable = happybase.Table('socialAttributesTable', connection)
consumptionFeaturesTable = happybase.Table('consumptionFeaturesTable', connection)
internetFeaturesTable = happybase.Table('internetFeaturesTable', connection)
consumptionCharacteristicsTable = happybase.Table('consumptionCharacteristicsTable', connection)
internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)


def plt(key):
    """连接hbase"""
    connection = happybase.Connection(host="localhost", port=9090)
    """打开传输"""
    connection.open()
    """连接表"""
    basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
    socialAttributesTable = happybase.Table('socialAttributesTable', connection)
    consumptionFeaturesTable = happybase.Table('consumptionFeaturesTable', connection)
    internetFeaturesTable = happybase.Table('internetFeaturesTable', connection)
    consumptionCharacteristicsTable = happybase.Table('consumptionCharacteristicsTable', connection)
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)

    """获取信息"""
    try:
        key_name = basicFeaturesTable.row(key, columns=["name"])
    except:
        """连接hbase"""
        connection = happybase.Connection(host="localhost", port=9090)
        """打开传输"""
        connection.open()
        """连接表"""
        basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
        socialAttributesTable = happybase.Table('socialAttributesTable', connection)
        consumptionFeaturesTable = happybase.Table('consumptionFeaturesTable', connection)
        internetFeaturesTable = happybase.Table('internetFeaturesTable', connection)
        consumptionCharacteristicsTable = happybase.Table('consumptionCharacteristicsTable', connection)
        internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)

        key_name = basicFeaturesTable.row(key, columns=["name"])

    key_name = list(key_name.values())
    key_name = key_name[0].decode()

    try:
        sInfo = socialAttributesTable.row(key, columns=["incLevel", "eduLevel"])
        cInfo = consumptionFeaturesTable.row(key, columns=["CMLevel", "CTLevel"])
        iInfo = internetFeaturesTable.row(key, columns=["intLevel"])
        consumptionCharacteristicsInfo = consumptionCharacteristicsTable.row(key,
                                                                             columns=["commodity", "price", "date"])
        internetBehaviorInfo = internetBehaviorTable.row \
            (key, columns=
            ["date", "news", "communications", "entertainment", "domersticServices", "busApp", "toolUse"]
             )
    except:
        """连接hbase"""
        connection = happybase.Connection(host="localhost", port=9090)
        """打开传输"""
        connection.open()
        """连接表"""
        basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
        socialAttributesTable = happybase.Table('socialAttributesTable', connection)
        consumptionFeaturesTable = happybase.Table('consumptionFeaturesTable', connection)
        internetFeaturesTable = happybase.Table('internetFeaturesTable', connection)
        consumptionCharacteristicsTable = happybase.Table('consumptionCharacteristicsTable', connection)
        internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)

        sInfo = socialAttributesTable.row(key, columns=["incLevel", "eduLevel"])
        cInfo = consumptionFeaturesTable.row(key, columns=["CMLevel", "CTLevel"])
        iInfo = internetFeaturesTable.row(key, columns=["intLevel"])
        consumptionCharacteristicsInfo = consumptionCharacteristicsTable.row(key,
                                                                             columns=["commodity", "price", "date"])
        internetBehaviorInfo = internetBehaviorTable.row \
            (key, columns=
            ["date", "news", "communications", "entertainment", "domersticServices", "busApp", "toolUse"]
             )

    """消费情况图"""
    consumptionCharacteristicsValues = list(consumptionCharacteristicsInfo.values())
    consumptionCharacteristicsValues = list(map(lambda x: x.decode(), consumptionCharacteristicsValues))

    consumptionCharacteristicsValues[
    int(len(consumptionCharacteristicsValues) / 3):-int(len(consumptionCharacteristicsValues) / 3)] \
        = list(map(int, consumptionCharacteristicsValues[int(len(consumptionCharacteristicsValues) / 3):
                                                         -int(len(consumptionCharacteristicsValues) / 3)]))
    consumptionCharacteristicsValues[-int(len(consumptionCharacteristicsValues) / 3):] \
        = list(map(int, consumptionCharacteristicsValues[-int(len(consumptionCharacteristicsValues) / 3):]))

    consumptionCharacteristicsData = []
    consumptionCharacteristicsData.append(
        consumptionCharacteristicsValues[:int(len(consumptionCharacteristicsValues) / 3)])
    consumptionCharacteristicsData.append(
        consumptionCharacteristicsValues[int(len(consumptionCharacteristicsValues) / 3):
                                         -int(len(consumptionCharacteristicsValues) / 3)])
    consumptionCharacteristicsData.append(
        consumptionCharacteristicsValues[-int(len(consumptionCharacteristicsValues) / 3):])

    consumptionCharacteristicsData = pd.DataFrame(consumptionCharacteristicsData).T
    consumptionCharacteristicsData.columns = ["commodity", "date", "price"]
    consumptionCharacteristicsData.sort_values(by="date", inplace=True)
    consumptionCharacteristicsData_y = consumptionCharacteristicsData.loc[:, "date"].tolist()
    consumptionCharacteristicsData_y = list(map(str, consumptionCharacteristicsData_y))
    consumptionCharacteristicsData_x = consumptionCharacteristicsData.loc[:, "price"].tolist()
    consumptionCharacteristicsData_commodity = consumptionCharacteristicsData.loc[:, "commodity"].tolist()

    quzhong_consumptionCharacteristicsData_y = list(set(consumptionCharacteristicsData_y))
    if len(quzhong_consumptionCharacteristicsData_y) != len(consumptionCharacteristicsData_y):
        for i in range(len(quzhong_consumptionCharacteristicsData_y)):
            count_consumptionCharacteristicsData_y = consumptionCharacteristicsData_y. \
                count(quzhong_consumptionCharacteristicsData_y[i])
            if count_consumptionCharacteristicsData_y != 1:
                index = consumptionCharacteristicsData_y.index(quzhong_consumptionCharacteristicsData_y[i])
                consumptionCharacteristicsData_y = consumptionCharacteristicsData_y[:index + 1] + \
                                                   consumptionCharacteristicsData_y[
                                                   index + count_consumptionCharacteristicsData_y:]
                consumptionCharacteristicsData_x[index] = sum(consumptionCharacteristicsData_x
                                                              [index:index + count_consumptionCharacteristicsData_y])
                consumptionCharacteristicsData_x = consumptionCharacteristicsData_x[:index + 1] + \
                                                   consumptionCharacteristicsData_x[
                                                   index + count_consumptionCharacteristicsData_y:]
                consumptionCharacteristicsData_commodity[index] = \
                    ",".join(
                        consumptionCharacteristicsData_commodity[index:index + count_consumptionCharacteristicsData_y])
                consumptionCharacteristicsData_commodity \
                    = consumptionCharacteristicsData_commodity[:index + 1] + \
                      consumptionCharacteristicsData_commodity[index + count_consumptionCharacteristicsData_y:]

    consumptionCharacteristicsData_xx = \
        [list(z) for z in zip(consumptionCharacteristicsData_x, consumptionCharacteristicsData_commodity)]

    scatter = Scatter(init_opts=opts.InitOpts(width="850px", height="380px"))
    scatter.add_xaxis(consumptionCharacteristicsData_y)
    scatter.add_yaxis("", consumptionCharacteristicsData_xx, color="red")
    scatter.set_global_opts(
        tooltip_opts=opts.TooltipOpts(
            trigger="item",
            axis_pointer_type="cross",
            formatter=JsCode(
                "function (params) {return '消费日期：' + params.name + ' <br/>消费金额：' + params.value[1] + '元 <br/>消费产品：' + params.value[2];}"
            )
        ),
        yaxis_opts=opts.AxisOpts(
            name="消费金额",
            type_="value",
            name_textstyle_opts=opts.TextStyleOpts(color="white"),  ###########
            axislabel_opts=opts.LabelOpts(formatter="{value} 元", border_color="white", color="white"),  ########
            axistick_opts=opts.AxisTickOpts(is_show=True),
            splitline_opts=opts.SplitLineOpts(is_show=True),
        ),
        xaxis_opts=opts.AxisOpts(
            name="消费日期",
            type_="category",
            name_textstyle_opts=opts.TextStyleOpts(color="white"),  ###########
            axislabel_opts=opts.LabelOpts(border_color="white", color="white"),  ##############
            axispointer_opts=opts.AxisPointerOpts(is_show=True, type_="shadow"),
        ),
        legend_opts=opts.LegendOpts(is_show=False)
    )
    scatter.set_series_opts(label_opts=opts.LabelOpts(is_show=False))

    line = Line(init_opts=opts.InitOpts(width="850px", height="380px"))
    line.add_xaxis(consumptionCharacteristicsData_y)
    line.add_yaxis(series_name="", y_axis=consumptionCharacteristicsData_x, color="red")

    scatter.overlap(line)
    scatter.render("./static/html/pictureConsumptionCharacteristics.html")

    """互联网行为图"""
    internetBehaviorInfoKeys = list(internetBehaviorInfo.keys())
    internetBehaviorInfoKeys = list(map(lambda x: x.decode(), internetBehaviorInfoKeys))
    internetBehaviorInfoKeys = list(map(lambda x: x[:x.index(':')], internetBehaviorInfoKeys))
    internetBehaviorInfoValues = list(internetBehaviorInfo.values())
    internetBehaviorInfoValues = list(map(lambda x: x.decode(), internetBehaviorInfoValues))
    internetBehaviorInfoValues = list(map(int, internetBehaviorInfoValues))

    internetBehaviorData = []
    splitNum = int(len(internetBehaviorInfoValues) / 7)
    internetBehaviorData.append(internetBehaviorInfoValues[:splitNum])
    internetBehaviorData.append(internetBehaviorInfoValues[splitNum:2 * splitNum])
    internetBehaviorData.append(internetBehaviorInfoValues[2 * splitNum:3 * splitNum])
    internetBehaviorData.append(internetBehaviorInfoValues[3 * splitNum:4 * splitNum])
    internetBehaviorData.append(internetBehaviorInfoValues[4 * splitNum:5 * splitNum])
    internetBehaviorData.append(internetBehaviorInfoValues[5 * splitNum:6 * splitNum])
    internetBehaviorData.append(internetBehaviorInfoValues[6 * splitNum:7 * splitNum])

    internetBehaviorData = pd.DataFrame(internetBehaviorData).T
    internetBehaviorData.columns = [internetBehaviorInfoKeys[0], internetBehaviorInfoKeys[splitNum],
                                    internetBehaviorInfoKeys[2 * splitNum], internetBehaviorInfoKeys[3 * splitNum],
                                    internetBehaviorInfoKeys[4 * splitNum], internetBehaviorInfoKeys[5 * splitNum],
                                    internetBehaviorInfoKeys[6 * splitNum]]

    internetBehaviorData.sort_values(by="date", inplace=True)
    internetBehaviorData_y = internetBehaviorData.loc[:, "date"].tolist()
    internetBehaviorData_y = list(map(str, internetBehaviorData_y))

    bar = Bar(init_opts=opts.InitOpts(width="800px", height="380px"))
    bar.add_xaxis(internetBehaviorData_y)
    bar.add_yaxis(
        series_name="新闻资讯",
        yaxis_data=internetBehaviorData.loc[:, "news"].tolist(),
        stack="stack"
    )
    bar.add_yaxis(
        series_name="通信交流",
        yaxis_data=internetBehaviorData.loc[:, "communications"].tolist(),
        stack="stack"
    )
    bar.add_yaxis(
        series_name="娱乐休闲",
        yaxis_data=internetBehaviorData.loc[:, "entertainment"].tolist(),
        stack="stack"
    )
    bar.add_yaxis(
        series_name="生活服务",
        yaxis_data=internetBehaviorData.loc[:, "domersticServices"].tolist(),
        stack="stack"
    )
    bar.add_yaxis(
        series_name="商务应用",
        yaxis_data=internetBehaviorData.loc[:, "busApp"].tolist(),
        stack="stack"
    )
    bar.add_yaxis(
        series_name="工具使用",
        yaxis_data=internetBehaviorData.loc[:, "toolUse"].tolist(),
        stack="stack"
    )
    bar.set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    bar.set_global_opts(
        tooltip_opts=opts.TooltipOpts(
            is_show=True, trigger="axis", axis_pointer_type="cross"
        ),
        xaxis_opts=opts.AxisOpts(
            name="日期",
            name_textstyle_opts=opts.TextStyleOpts(color="white"),  ###########
            type_="category",
            axislabel_opts=opts.LabelOpts(border_color="white", color="white"),  #############
            axispointer_opts=opts.AxisPointerOpts(is_show=True),
        ),
        yaxis_opts=opts.AxisOpts(
            name="时间",
            name_textstyle_opts=opts.TextStyleOpts(color="white"),
            type_="value",
            min_=0,
            max_=24,
            interval=4,
            axislabel_opts=opts.LabelOpts(formatter="{value} 小时", border_color="white", color="white"),  ###########
            axistick_opts=opts.AxisTickOpts(is_show=True),

        ),
        datazoom_opts=opts.DataZoomOpts(type_="inside"),
        legend_opts=opts.LegendOpts(
            is_show=True,
            textstyle_opts=opts.TextStyleOpts(border_color="white", color="wihte"),
            orient='horizontal'
        )
    )
    bar.render("./static/html/pictureInternetBehavior.html")

    """个人特征图"""
    sValue = list(sInfo.values())
    sValue = list(map(lambda x: x.decode(), sValue))
    cValue = list(cInfo.values())
    cValue = list(map(lambda x: x.decode(), cValue))
    iValue = list(iInfo.values())
    iValue = list(map(lambda x: x.decode(), iValue))
    value = sValue + cValue + iValue
    value = list(map(int, value))

    value = [value[1], value[2], value[3], value[0], value[4]]
    value = [value]

    rader = Radar(init_opts=opts.InitOpts(width="350px", height="350px"))
    rader.add_schema(
        schema=[
            opts.RadarIndicatorItem(name="收入指数", max_=5),
            opts.RadarIndicatorItem(name="消费金\n额指数", max_=5),
            opts.RadarIndicatorItem(name="消费次数指数", max_=5),
            opts.RadarIndicatorItem(name="学历指数", max_=5),
            opts.RadarIndicatorItem(name="网络依\n赖指数", max_=5),
        ],
        shape='polygon'
    )
    rader.add(series_name=key_name, data=value)
    rader.set_global_opts(legend_opts=opts.LegendOpts(is_show=False))
    rader.render("./static/html/pictureCompositiveInfo.html")
