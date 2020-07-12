import happybase
import pandas as pd

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()

"""连接表"""
basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
consumptionCharacteristicsTable = happybase.Table('consumptionCharacteristicsTable', connection)

# 分地区求
rowsNumData = pd.read_csv("data/城市每个人的key.csv")

types = ["学习", "衣服", "家电", "鞋子", "电子产品", "食品"]
sexes = ["男", "女"]

"""每个地区的特征数据"""
data = []

for i in range(rowsNumData.shape[0]):
    types_man_num = [0 for x in range(6)]
    types_woman_num = [0 for x in range(6)]
    types_15_25_num = [0 for x in range(6)]
    types_25_35_num = [0 for x in range(6)]
    types_35_45_num = [0 for x in range(6)]
    types_over_45_num = [0 for x in range(6)]

    rowsNum = eval(rowsNumData.iloc[i, 1])

    for j in rowsNum:
        bInfo = basicFeaturesTable.row(j, columns=["age", "sex"])
        bValues = list(bInfo.values())
        bValues = list(map(lambda x: x.decode(), bValues))
        bValues[0] = int(bValues[0])

        cInfo = consumptionCharacteristicsTable.row(j, columns=["type"])
        cValues = list(cInfo.values())
        cValues = list(map(lambda x: x.decode(), cValues))

        # sex_type
        if bValues[1] == "男":
            for k in cValues:
                for l in range(len(types)):
                    if k == types[l]:
                        types_man_num[l] += 1
                        break
        else:
            for k in cValues:
                for l in range(len(types)):
                    if k == types[l]:
                        types_woman_num[l] += 1
                        break

        # age_type
        if bValues[0] < 25:
            for k in cValues:
                for l in range(len(types)):
                    if k == types[l]:
                        types_15_25_num[l] += 1
                        break
        elif bValues[0] < 35:
            for k in cValues:
                for l in range(len(types)):
                    if k == types[l]:
                        types_25_35_num[l] += 1
                        break
        elif bValues[0] < 45:
            for k in cValues:
                for l in range(len(types)):
                    if k == types[l]:
                        types_35_45_num[l] += 1
                        break
        else:
            for k in cValues:
                for l in range(len(types)):
                    if k == types[l]:
                        types_over_45_num[l] += 1
                        break
    print(rowsNumData.iloc[i, 0])
    data.append([rowsNumData.iloc[i, 0], types_man_num, types_woman_num, types_15_25_num,
                 types_25_35_num, types_35_45_num, types_over_45_num])

data = pd.DataFrame(data)
data.columns = ["place", "man_type", "woman_type", "15_25_type",
                "25_35_type", "35_45_type", "over_45_type"]

data.to_csv("data/各省消费特征数据.csv", index=None)
data.to_csv("../static/data/各省消费特征数据.csv", index=None)
