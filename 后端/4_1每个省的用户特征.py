import happybase
import pandas as pd

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()

"""连接表"""
basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
socialAttributesTable = happybase.Table('socialAttributesTable', connection)
consumptionFeaturesTable = happybase.Table('consumptionFeaturesTable', connection)
internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)

industry = ['服务业', '汽车业', '餐饮业', '金融业', '旅游业', '教育业', '销售业', '化学业', '管理业', '土木业', '建筑业',
            '计算机业', '医疗业', '设计业', '媒体业', '制造业', '会计业', '广告业', '服装业', '运输业', '公共关系业']
education = ["初中", "高中", "专科", "本科", "硕士", "博士"]

"""每个地区的特征数据"""
ages = []
manNums = []
womanNums = []
educations = []
maxIncomes = []
minIncomes = []
avgIncome = []
industrys = []
internetTime = []

rowsNumData = pd.read_csv("data/城市每个人的key.csv")

for i in range(rowsNumData.shape[0]):
    rowsNum = eval(rowsNumData.iloc[i, 1])
    manNum = 0
    womanNum = 0
    age1 = 0
    age2 = 0
    age3 = 0
    age4 = 0
    age5 = 0
    age6 = 0
    age7 = 0
    age8 = 0
    age9 = 0
    tempIndustry = [0 for l in range(len(industry))]
    tempIncome = []
    tempEducation = [0 for l in range(len(education))]
    for j in rowsNum:
        bInfo = basicFeaturesTable.row(j, columns=["age", "sex"])
        bValues = list(bInfo.values())
        bValues = list(map(lambda x: x.decode(), bValues))
        bValues[0] = int(bValues[0])
        if bValues[1] == "男":
            manNum += 1
        else:
            womanNum += 1
        if bValues[0] <= 20:
            age1 += 1
        elif bValues[0] <= 25:
            age2 += 1
        elif bValues[0] <= 30:
            age3 += 1
        elif bValues[0] <= 35:
            age4 += 1
        elif bValues[0] <= 40:
            age5 += 1
        elif bValues[0] <= 45:
            age6 += 1
        elif bValues[0] <= 50:
            age7 += 1
        elif bValues[0] <= 55:
            age8 += 1
        else:
            age9 += 1

        sInfo = socialAttributesTable.row(j, columns=["industry", "income", "education"])
        sValues = list(sInfo.values())
        sValues = list(map(lambda x: x.decode(), sValues))
        sValues[1] = eval(sValues[1])
        tempIncome.append(sValues[1])
        for l in range(len(industry)):
            if sValues[2] == industry[l]:
                tempIndustry[l] += 1
        for l in range(len(education)):
            if sValues[0] == education[l]:
                tempEducation[l] += 1

    ages.append([age1, age2, age3, age4, age5, age6, age7, age8, age9])
    manNums.append(manNum)
    womanNums.append(womanNum)
    educations.append(tempEducation)
    maxIncomes.append(round(max(tempIncome)))
    minIncomes.append(round(min(tempIncome)))
    avgIncome.append(round(sum(tempIncome) / len(rowsNum)))
    industrys.append(tempIndustry)

    tempInternetTime = [0 for j in range(6)]
    internetBehaviorInfo = internetBehaviorTable.rows(rowsNum, columns=['busApp'])
    for j in internetBehaviorInfo:
        values = list(j[1].values())
        values = list(map(lambda x: x.decode(), values))
        values = list(map(eval, values))
        tempInternetTime[0] += sum(values)
    internetBehaviorInfo = internetBehaviorTable.rows(rowsNum, columns=['communications'])
    for j in internetBehaviorInfo:
        values = list(j[1].values())
        values = list(map(lambda x: x.decode(), values))
        values = list(map(eval, values))
        tempInternetTime[1] += sum(values)
    internetBehaviorInfo = internetBehaviorTable.rows(rowsNum, columns=['domersticServices'])
    for j in internetBehaviorInfo:
        values = list(j[1].values())
        values = list(map(lambda x: x.decode(), values))
        values = list(map(eval, values))
        tempInternetTime[2] += sum(values)
    internetBehaviorInfo = internetBehaviorTable.rows(rowsNum, columns=['entertainment'])
    for j in internetBehaviorInfo:
        values = list(j[1].values())
        values = list(map(lambda x: x.decode(), values))
        values = list(map(eval, values))
        tempInternetTime[3] += sum(values)
    internetBehaviorInfo = internetBehaviorTable.rows(rowsNum, columns=['news'])
    for j in internetBehaviorInfo:
        values = list(j[1].values())
        values = list(map(lambda x: x.decode(), values))
        values = list(map(eval, values))
        tempInternetTime[4] += sum(values)
    internetBehaviorInfo = internetBehaviorTable.rows(rowsNum, columns=['toolUse'])
    for j in internetBehaviorInfo:
        values = list(j[1].values())
        values = list(map(lambda x: x.decode(), values))
        values = list(map(eval, values))
        tempInternetTime[5] += sum(values)

    tempInternetTime = list(map(lambda x: x / len(internetBehaviorInfo), tempInternetTime))
    internetTime.append(tempInternetTime)
    print(rowsNumData.iloc[i, 0])

place = rowsNumData.iloc[:, 0].to_list()

data = pd.DataFrame(
    [place, avgIncome, ages, educations, maxIncomes, minIncomes, industrys, internetTime, manNums, womanNums]).T
data.columns = ["place", "avgIncome", "age",
                "edu", "maxIncome", "minIncome", "job",
                "sur_time", "manNum", "womanNum"]

data.to_csv("data/各省用户特征数据.csv", index=None)
data.to_csv("../static/data/各省用户特征数据.csv", index=None)
