import happybase
import pandas as pd

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()

"""创建表"""
if b'consumptionFeaturesTable' in connection.tables():
    connection.delete_table('consumptionFeaturesTable', disable=True)

connection.create_table(
    'consumptionFeaturesTable',
    {'CSMoney': dict(), 'CSTimes': dict(), 'CMLevel': dict(), 'CTLevel': dict(),
     'FCType': dict(), 'FCMoney': dict(), 'SCType': dict(), 'SCMoney': dict(), 'character': dict()}
)

"""连接表"""
consumptionFeaturesTable = happybase.Table('consumptionFeaturesTable', connection)
consumptionFeaturesTableBat = consumptionFeaturesTable.batch(batch_size=1000)

consumptionCharacteristicsTable = happybase.Table('consumptionCharacteristicsTable', connection)

"""消费情况表 consumptionCharacteristicsTable 分析"""
consumptionCharacteristicsTable_priceBZ = pd.read_csv("data/最高最低消费金额与次数.csv")
maxConsumeMoney = int(consumptionCharacteristicsTable_priceBZ.iloc[0, 0])
minConsumeMoney = int(consumptionCharacteristicsTable_priceBZ.iloc[1, 0])
consumeMoneyCha = (maxConsumeMoney - minConsumeMoney) / 5
maxConsumeTimes = int(consumptionCharacteristicsTable_priceBZ.iloc[0, 1])
minConsumeTimes = int(consumptionCharacteristicsTable_priceBZ.iloc[1, 1])
consumeTimesCha = (maxConsumeTimes - minConsumeTimes) / 5

consumptionCharacteristicsTableScanner = consumptionCharacteristicsTable.scan(columns=["price", "type"])
for k, v in consumptionCharacteristicsTableScanner:
    value = []
    key = k.decode()
    values = list(v.values())
    values = list(map(lambda x: x.decode(), values))
    consumeMoney = values[:int(len(values) / 2)]
    consumeMoney = list(map(int, consumeMoney))
    sumConsumeMoney = round(sum(consumeMoney))
    consumeTimes = len(consumeMoney)
    value.append(str(sumConsumeMoney))
    value.append(str(consumeTimes))
    types = values[int(len(values) / 2):]
    consumeMoneyLevel = 0
    if sumConsumeMoney < minConsumeMoney + consumeMoneyCha:
        consumeMoneyLevel = 1
    elif sumConsumeMoney < minConsumeMoney + 2 * consumeMoneyCha:
        consumeMoneyLevel = 2
    elif sumConsumeMoney < minConsumeMoney + 3 * consumeMoneyCha:
        consumeMoneyLevel = 3
    elif sumConsumeMoney < minConsumeMoney + 4 * consumeMoneyCha:
        consumeMoneyLevel = 4
    else:
        consumeMoneyLevel = 5
    value.append(str(consumeMoneyLevel))
    consumeTimesLevel = 0
    if consumeTimes < minConsumeTimes + consumeTimesCha:
        consumeTimesLevel = 1
    elif consumeTimes < minConsumeTimes + 2 * consumeTimesCha:
        consumeTimesLevel = 2
    elif consumeTimes < minConsumeTimes + 3 * consumeTimesCha:
        consumeTimesLevel = 3
    elif consumeTimes < minConsumeTimes + 4 * consumeTimesCha:
        consumeTimesLevel = 4
    else:
        consumeTimesLevel = 5
    value.append(str(consumeTimesLevel))
    quzhongTypes = list(set(types))
    data = pd.DataFrame([types, consumeMoney]).T
    sumTypesConsumeMoney = []
    if len(quzhongTypes) == 1:
        for i in range(len(quzhongTypes)):
            sumTypesConsumeMoney.append(data[data.iloc[:, 0] == quzhongTypes[i]].iloc[:, 1].sum())
        data = pd.DataFrame([quzhongTypes, sumTypesConsumeMoney]).T
        data.sort_values(by=1, inplace=True, ascending=False)
        value.append(data.iloc[0, 0])
        value.append(str(round(data.iloc[0, 1])))
        value.append(None)
        value.append(None)
    else:
        for i in range(len(quzhongTypes)):
            sumTypesConsumeMoney.append(data[data.iloc[:, 0] == quzhongTypes[i]].iloc[:, 1].sum())
        data = pd.DataFrame([quzhongTypes, sumTypesConsumeMoney]).T
        data.sort_values(by=1, inplace=True, ascending=False)
        value.append(data.iloc[0, 0])
        value.append(str(round(data.iloc[0, 1])))
        value.append(data.iloc[1, 0])
        value.append(str(round(data.iloc[1, 1])))
    character = ''
    if '电子产品' in quzhongTypes:
        character += ' 电子产品爱好者'
    if '家电' in quzhongTypes:
        character += ' 居家达人'
    if value[4] == '鞋子':
        character += ' 爱鞋狂人'
    if value[4] == '衣服':
        character += ' 时尚达人'
    if value[4] == '食品':
        character += ' 吃货'
    if value[4] == '学习':
        character += ' 上进王'
    value.append(character)
    consumptionFeaturesTableBat.put(key,
                                    {'CSMoney:0': value[0], 'CSTimes:0': value[1], 'CMLevel:0': value[2],
                                     'CTLevel:0': value[3], 'FCType:0': value[4], 'FCMoney:0': value[5],
                                     'SCType:0': value[6], 'SCMoney:0': value[7], 'character:0': value[8]}
                                    )

consumptionFeaturesTableBat.send()
print("消费情况表 consumptionCharacteristicsTable 完成！")
