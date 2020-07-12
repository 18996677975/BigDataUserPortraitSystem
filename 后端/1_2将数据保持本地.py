import happybase
import pandas as pd

# 连接 HBase
connection = happybase.Connection(host="localhost", port=9090)
connection.open()

# 连接表
basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
socialAttributesTable = happybase.Table('socialAttributesTable', connection)
consumptionCharacteristicsTable = happybase.Table('consumptionCharacteristicsTable', connection)
internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)


# # basicFeatures
# basicFeaturesData = []
#
# scanner = basicFeaturesTable.scan(columns=['name', 'sex', 'age', 'height', 'wight', 'place'])
# for k, v in scanner:
#     key = k.decode()
#     tempData = [key]
#     keys = list(v.keys())
#     keys = list(map(lambda x: x.decode(), keys))
#     values = list(v.values())
#     values = list(map(lambda x: x.decode(), values))
#     for i in range(len(values)):
#         temp = [keys[i], values[i]]
#         tempData.append(temp)
#     basicFeaturesData.append(tempData)
#
# basicFeaturesData = pd.DataFrame(basicFeaturesData)
# basicFeaturesData.columns = ['key', 'age', 'height', 'name', 'place', 'sex', 'wight']
# basicFeaturesData.to_csv("data/basicFeaturesData.csv", index=None)


# # socialAttributes
# socialAttributesData = []
#
# scanner = socialAttributesTable.scan(columns=['industry', 'occupation', 'education', 'income'])
# for k, v in scanner:
#     key = k.decode()
#     tempData = [key]
#     keys = list(v.keys())
#     keys = list(map(lambda x: x.decode(), keys))
#     values = list(v.values())
#     values = list(map(lambda x: x.decode(), values))
#     for i in range(len(values)):
#         temp = [keys[i], values[i]]
#         tempData.append(temp)
#     socialAttributesData.append(tempData)
#
# socialAttributesData = pd.DataFrame(socialAttributesData)
# socialAttributesData.columns = ['key', 'education', 'income', 'industry', 'occupation']
# socialAttributesData.to_csv("data/socialAttributesData.csv", index=None)


# # socialAttributes
# consumptionCharacteristicsData = []
#
# scanner = consumptionCharacteristicsTable.scan()
# for k, v in scanner:
#     key = k.decode()
#     keys = list(v.keys())
#     keys = list(map(lambda x: x.decode(), keys))
#     values = list(v.values())
#     values = list(map(lambda x: x.decode(), values))
#     index = int(len(values) / 4)
#     commodity = []
#     date = []
#     price = []
#     types = []
#     for i in range(index):
#         temp1 = [keys[i], values[i]]
#         temp2 = [keys[index+i], values[index+i]]
#         temp3 = [keys[2*index + i], values[2*index + i]]
#         temp4 = [keys[3*index + i], values[3*index + i]]
#         commodity.append(temp1)
#         date.append(temp2)
#         price.append(temp3)
#         types.append(temp4)
#     consumptionCharacteristicsData.append([key, commodity, date, price, types])
#
# consumptionCharacteristicsData = pd.DataFrame(consumptionCharacteristicsData)
# consumptionCharacteristicsData.columns = ['key', 'commodity', 'date', 'price', 'type']
# consumptionCharacteristicsData.to_csv("data/consumptionCharacteristicsData.csv", index=None)


# internetBehavior
internetBehaviorData = []

try:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    intKeys = []
    busApps = []
    scanner = internetBehaviorTable.scan(columns=['busApp'])
    for k, v in scanner:
        key = k.decode()
        intKeys.append(key)
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        busApp = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            busApp.append(temp)
        busApps.append(busApp)
except:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    intKeys = []
    busApps = []
    scanner = internetBehaviorTable.scan(columns=['busApp'])
    for k, v in scanner:
        key = k.decode()
        intKeys.append(key)
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        busApp = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            busApp.append(temp)
        busApps.append(busApp)

try:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    communicationss = []
    scanner = internetBehaviorTable.scan(columns=['communications'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        communications = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            communications.append(temp)
        communicationss.append(communications)
except:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    communicationss = []
    scanner = internetBehaviorTable.scan(columns=['communications'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        communications = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            communications.append(temp)
        communicationss.append(communications)

try:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    dates = []
    scanner = internetBehaviorTable.scan(columns=['date'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        date = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            date.append(temp)
        dates.append(date)
except:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    dates = []
    scanner = internetBehaviorTable.scan(columns=['date'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        date = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            date.append(temp)
        dates.append(date)

try:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    domersticServicess = []
    scanner = internetBehaviorTable.scan(columns=['domersticServices'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        domersticServices = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            domersticServices.append(temp)
        domersticServicess.append(domersticServices)
except:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    domersticServicess = []
    scanner = internetBehaviorTable.scan(columns=['domersticServices'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        domersticServices = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            domersticServices.append(temp)
        domersticServicess.append(domersticServices)

try:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    entertainments = []
    scanner = internetBehaviorTable.scan(columns=['entertainment'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        entertainment = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            entertainment.append(temp)
        entertainments.append(entertainment)
except:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    entertainments = []
    scanner = internetBehaviorTable.scan(columns=['entertainment'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        entertainment = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            entertainment.append(temp)
        entertainments.append(entertainment)

try:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    newss = []
    scanner = internetBehaviorTable.scan(columns=['news'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        news = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            news.append(temp)
        newss.append(news)
except:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    newss = []
    scanner = internetBehaviorTable.scan(columns=['news'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        news = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            news.append(temp)
        newss.append(news)

try:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    toolUses = []
    scanner = internetBehaviorTable.scan(columns=['toolUse'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        toolUse = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            toolUse.append(temp)
        toolUses.append(toolUse)
except:
    internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
    toolUses = []
    scanner = internetBehaviorTable.scan(columns=['toolUse'])
    for k, v in scanner:
        key = k.decode()
        keys = list(v.keys())
        keys = list(map(lambda x: x.decode(), keys))
        values = list(v.values())
        values = list(map(lambda x: x.decode(), values))
        toolUse = []
        for i in range(len(values)):
            temp = [keys[i], values[i]]
            toolUse.append(temp)
        toolUses.append(toolUse)

for i in range(len(intKeys)):
    internetBehaviorData.append([intKeys[i], busApps[i], communicationss[i], dates[i],
                                 domersticServicess[i], entertainments[i], newss[i], toolUses[i]])

internetBehaviorData = pd.DataFrame(internetBehaviorData)
internetBehaviorData.columns = ['key', 'busApp', 'communications', 'date',
                                'domersticServices', 'entertainment', 'news', 'toolUse']
internetBehaviorData.to_csv("data/internetBehaviorData.csv", index=None)