import pandas as pd
import pyecharts.options as opts
from pyecharts.globals import ThemeType
from pyecharts.commons.utils import JsCode
from pyecharts.charts import Grid, Bar, Map, Pie

data = pd.read_csv("data/各省用户特征数据.csv")
data = data.loc[:, ["place", "avgIncome"]]

data.sort_values(by="avgIncome", inplace=True, ascending=False)
sumIncome = data.iloc[:,1].sum()
data["incomePercent"] = data.iloc[:, 1] / sumIncome

data1 = []
for i in range(data.shape[0]):
    tempName = str(data.iloc[i, 0])
    tempData = [float(data.iloc[i, 1]), float(data.iloc[i, 2]), str(data.iloc[i, 0])]
    data1.append({"name": tempName, "value": tempData})

map_data = [
    [[x["name"], x["value"]] for x in data1]
][0]

min_data, max_data = (
    min([d[1][0] for d in map_data]),
    max([d[1][0] for d in map_data]),
)

map_chart = (
    Map(init_opts=opts.InitOpts())
        .add(
        series_name="",
        data_pair=map_data,
        label_opts=opts.LabelOpts(is_show=False),
        is_map_symbol_show=False,
        itemstyle_opts={
            "normal": {"areaColor": "#323c48", "borderColor": "#404a59"},
        },
    )
        .set_global_opts(
        title_opts=opts.TitleOpts(
            title="中国各省人均收入排名情况",
            pos_left="center",
            pos_top="top",
            title_textstyle_opts=opts.TextStyleOpts(
                font_size=25, color="rgba(255,255,255, 0.9)"
            ),
        ),
        tooltip_opts=opts.TooltipOpts(
            is_show=True,
            formatter=JsCode(
                """function(params) {
                if ('value' in params.data) {
                    return params.data.value[2] + ': ' + params.data.value[0];
                }
            }"""
            ),
        ),
        visualmap_opts=opts.VisualMapOpts(
            is_calculable=True,
            dimension=0,
            pos_left="10",
            pos_top="25%",
            range_text=["High", "Low"],
            range_color=["lightskyblue", "yellow", "orangered"],
            textstyle_opts=opts.TextStyleOpts(color="#ddd"),
            min_=min_data,
            max_=max_data,
        ),
    )
)

bar_x_data = [x[0] for x in map_data]

bar_y_data = [{"name": x[0], "value": x[1][0]} for x in map_data]
bar = (
    Bar()
        .add_xaxis(xaxis_data=bar_x_data)
        .add_yaxis(
        series_name="",
        yaxis_index=1,
        yaxis_data=bar_y_data,
        label_opts=opts.LabelOpts(
            is_show=True, position="right", formatter="{b}: {c}"
        ),
    )
        .reversal_axis()
        .set_global_opts(
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False)),
        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False)),
        tooltip_opts=opts.TooltipOpts(is_show=False),
        visualmap_opts=opts.VisualMapOpts(
            is_calculable=True,
            dimension=0,
            pos_left="10",
            pos_top="25%",
            range_text=["High", "Low"],
            range_color=["lightskyblue", "yellow", "orangered"],
            textstyle_opts=opts.TextStyleOpts(color="#ddd"),
            min_=min_data,
            max_=max_data,
        ),
    )
)

pie_data = [[x[0], x[1][0]] for x in map_data]
percent_sum = sum([x[1][1] for x in map_data])
rest_value = 0

pie = (
    Pie()
        .add(
        series_name="",
        data_pair=pie_data,
        radius=["12%", "20%"],
        center=["88%", "50%"],
        itemstyle_opts=opts.ItemStyleOpts(
            border_width=1, border_color="rgba(0,0,0,0.3)"
        ),
    )
        .set_global_opts(
        tooltip_opts=opts.TooltipOpts(is_show=True, formatter="{b} {d}%"),
        legend_opts=opts.LegendOpts(is_show=False),
    )
)

grid_chart = (
    Grid(init_opts=opts.InitOpts(width="1912px", height="953px", theme=ThemeType.DARK))
        .add(
        bar,
        grid_opts=opts.GridOpts(
            pos_left="10", pos_right="70%", pos_top="50%", pos_bottom="5"
        ),
    )
        .add(pie, grid_opts=opts.GridOpts())
        .add(map_chart, grid_opts=opts.GridOpts())
)

grid_chart.render("../static/html/picturePlaceCharacteristics.html")