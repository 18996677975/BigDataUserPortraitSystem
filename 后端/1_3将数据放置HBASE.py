import pandas as pd
import happybase

""" HBase 准备 """
# 连接 HBase
connection = happybase.Connection(host="localhost", port=9090)
connection.open()
# 创建表（基础属性表 basicFeaturesTable，社会属性表 socialAttributesTable，消费情况表 consumptionCharacteristicsTable，互联网行为表 internetBehaviorTable）

# 基础属性表 basicFeaturesTable
if b'basicFeaturesTable' in connection.tables():
    connection.delete_table('basicFeaturesTable', disable=True)
connection.create_table('basicFeaturesTable', {'name': dict(), 'sex': dict(), 'age': dict(), 'height': dict(),
                                               'wight': dict(), 'place': dict(), 'bmi': dict()})
# 社会属性表 socialAttributesTable
if b'socialAttributesTable' in connection.tables():
    connection.delete_table('socialAttributesTable', disable=True)
connection.create_table('socialAttributesTable', {'industry': dict(), 'occupation': dict(), 'education': dict(),
                                                  'income': dict(), 'indLevel': dict(), 'eduLevel': dict(),
                                                  'incLevel': dict()})
# 消费情况表 consumptionCharacteristicsTable
if b'consumptionCharacteristicsTable' in connection.tables():
    connection.delete_table('consumptionCharacteristicsTable', disable=True)
connection.create_table('consumptionCharacteristicsTable',
                        {'commodity': dict(), 'type': dict(), 'price': dict(), 'date': dict()})
# 互联网行为表 internetBehaviorTable
if b'internetBehaviorTable' in connection.tables():
    connection.delete_table('internetBehaviorTable', disable=True)
connection.create_table('internetBehaviorTable',
                        {'news': dict(), 'communications': dict(), 'entertainment': dict(),
                         'domersticServices': dict(), 'busApp': dict(), 'toolUse': dict(), 'date': dict()})

"""连接表"""
basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
basicFeaturesTableBat = basicFeaturesTable.batch(batch_size=1000)
socialAttributesTable = happybase.Table('socialAttributesTable', connection)
socialAttributesTableBat = socialAttributesTable.batch(batch_size=1000)
consumptionCharacteristicsTable = happybase.Table('consumptionCharacteristicsTable', connection)
consumptionCharacteristicsTableBat = consumptionCharacteristicsTable.batch(batch_size=1000)
internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)
internetBehaviorTableBat = internetBehaviorTable.batch(batch_size=1000)


"""基础属性表 basicFeaturesTable"""
data = pd.read_csv("data/basicFeaturesData.csv")

for i in range(data.shape[0]):
    basicFeaturesTableBat.put("%s" % data.loc[i, 'key'], {eval(data.loc[i, 'age'])[0]: eval(data.loc[i, 'age'])[1],
                                                          eval(data.loc[i, 'sex'])[0]: eval(data.loc[i, 'sex'])[1],
                                                          eval(data.loc[i, 'height'])[0]: eval(data.loc[i, 'height'])[1],
                                                          eval(data.loc[i, 'wight'])[0]: eval(data.loc[i, 'wight'])[1],
                                                          eval(data.loc[i, 'place'])[0]: eval(data.loc[i, 'place'])[1],
                                                          eval(data.loc[i, 'name'])[0]: eval(data.loc[i, 'name'])[1]})

basicFeaturesTableBat.send()
print("基础属性表 basicFeaturesTable，完！")

"""社会属性表 socialAttributesTable"""
data = pd.read_csv("data/socialAttributesData.csv")

for i in range(data.shape[0]):
    socialAttributesTableBat.put("%s" % data.loc[i, 'key'], {eval(data.loc[i, 'education'])[0]: eval(data.loc[i, 'education'])[1],
                                                          eval(data.loc[i, 'income'])[0]: eval(data.loc[i, 'income'])[1],
                                                          eval(data.loc[i, 'industry'])[0]: eval(data.loc[i, 'industry'])[1],
                                                          eval(data.loc[i, 'occupation'])[0]: eval(data.loc[i, 'occupation'])[1]})

socialAttributesTableBat.send()
print("社会属性表 socialAttributesTable，完！")

"""消费情况表 consumptionCharacteristicsTable"""
data = pd.read_csv("data/consumptionCharacteristicsData.csv")

for i in range(data.shape[0]):
    key = data.loc[i, 'key']
    nums = len(eval(data.loc[i, 'price']))
    for j in range(nums):
        consumptionCharacteristicsTableBat.put("%s" % key,
                                     {eval(data.loc[i, 'price'])[j][0]: eval(data.loc[i, 'price'])[j][1],
                                      eval(data.loc[i, 'commodity'])[j][0]: eval(data.loc[i, 'commodity'])[j][1],
                                      eval(data.loc[i, 'type'])[j][0]: eval(data.loc[i, 'type'])[j][1],
                                      eval(data.loc[i, 'date'])[j][0]: eval(data.loc[i, 'date'])[j][1]})

consumptionCharacteristicsTableBat.send()
print("消费情况表 consumptionCharacteristicsTable，完！")

"""互联网行为表 internetBehaviorTable"""
data = pd.read_csv("data/internetBehaviorData.csv")

for i in range(data.shape[0]):
    key = data.loc[i, 'key']
    nums = len(eval(data.loc[i, 'news']))
    for j in range(nums):
        internetBehaviorTableBat.put("%s" % key,
                                     {eval(data.loc[i, 'news'])[j][0]: eval(data.loc[i, 'news'])[j][1],
                                      eval(data.loc[i, 'communications'])[j][0]: eval(data.loc[i, 'communications'])[j][1],
                                      eval(data.loc[i, 'entertainment'])[j][0]: eval(data.loc[i, 'entertainment'])[j][1],
                                      eval(data.loc[i, 'domersticServices'])[j][0]: eval(data.loc[i, 'domersticServices'])[j][1],
                                      eval(data.loc[i, 'busApp'])[j][0]: eval(data.loc[i, 'busApp'])[j][1],
                                      eval(data.loc[i, 'toolUse'])[j][0]: eval(data.loc[i, 'toolUse'])[j][1],
                                      eval(data.loc[i, 'date'])[j][0]: eval(data.loc[i, 'date'])[j][1]})

internetBehaviorTableBat.send()
print("互联网行为表 internetBehaviorTable，完！")