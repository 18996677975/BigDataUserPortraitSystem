import happybase
import pandas as pd

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()

"""创建表"""
if b'internetFeaturesTable' in connection.tables():
    connection.delete_table('internetFeaturesTable', disable=True)

connection.create_table(
    'internetFeaturesTable',
    {'intTime': dict(), 'intLevel': dict(), 'FIType': dict(),
     'FITime': dict(), 'SIType': dict(), 'SITime': dict()}
)

"""连接表"""
internetFeaturesTable = happybase.Table('internetFeaturesTable', connection)
internetFeaturesTableBat = internetFeaturesTable.batch(batch_size=1000)

internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)

"""互联网行为表 internetBehaviorTable 分析"""
internetBehaviorTableScanner_internetBZ = pd.read_csv("data/最高最低互联网行为用时.csv")
maxInternetBehaviorTime = int(internetBehaviorTableScanner_internetBZ.iloc[0, 0])
minInternetBehaviorTime = int(internetBehaviorTableScanner_internetBZ.iloc[1, 0])
internetBehaviorTimeCha = (maxInternetBehaviorTime - minInternetBehaviorTime) / 5

intHehavior = ['busApp', 'communications', 'domersticServices', 'entertainment', 'news', 'toolUse']

internetBehaviorTableScanner = internetBehaviorTable.scan(columns=intHehavior)
for k, v in internetBehaviorTableScanner:
    value = []
    key = k.decode()
    values = list(v.values())
    values = list(map(lambda x: x.decode(), values))
    values = list(map(eval, values))
    internetTime = round(sum(values))
    value.append(str(internetTime))
    if internetTime < minInternetBehaviorTime + internetBehaviorTimeCha:
        internetLevel = 1
    elif internetTime < minInternetBehaviorTime + 2 * internetBehaviorTimeCha:
        internetLevel = 2
    elif internetTime < minInternetBehaviorTime + 3 * internetBehaviorTimeCha:
        internetLevel = 3
    elif internetTime < minInternetBehaviorTime + 4 * internetBehaviorTimeCha:
        internetLevel = 4
    else:
        internetLevel = 5
    value.append(str(internetLevel))

    name = []
    times = []
    for i in range(len(intHehavior)):
        name.append(intHehavior[i])
        times.append(round(sum(values[70 * i:70 * (i + 1)])))
    data = pd.DataFrame([name, times]).T
    data.columns = ["name", "time"]
    data.sort_values(by="time", ascending=False, inplace=True)
    value.append(str(data.iloc[0, 0]))
    value.append(str(data.iloc[0, 1]))
    value.append(str(data.iloc[1, 0]))
    value.append(str(data.iloc[1, 1]))
    internetFeaturesTableBat.put(key,
                                 {'intTime:0': value[0], 'intLevel:0': value[1],
                                  'FIType:0': value[2], 'FITime:0': value[3],
                                  'SIType:0': value[4], 'SITime:0': value[5]}
                                 )

internetFeaturesTableBat.send()
print("互联网行为表 internetBehaviorTable 完成！")
