import happybase
import pandas as pd

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()

"""连接表"""
socialAttributesTable = happybase.Table('socialAttributesTable', connection)
socialAttributesTableBat = socialAttributesTable.batch(batch_size=1000)

"""社会属性表 socialAttributesTable 分析"""
socialAttributesTable_incomeBZ = pd.read_csv("data/各行业最高最低收入表.csv")
maxIncome = socialAttributesTable_incomeBZ[socialAttributesTable_incomeBZ.loc[:, '行业名'] == '总收入'].iloc[0, 1]
minIncome = socialAttributesTable_incomeBZ[socialAttributesTable_incomeBZ.loc[:, '行业名'] == '总收入'].iloc[0, 2]
incomeCha = (maxIncome - minIncome) / 5

socialAttributesTableScanner = socialAttributesTable.scan(columns=["industry", "education", "income"])
for k, v in socialAttributesTableScanner:
    value = []
    key = k.decode()
    values = list(v.values())
    values = list(map(lambda x: x.decode(), values))
    incomeLevel = 0
    if eval(values[1]) < minIncome + incomeCha:
        incomeLevel = 1
    elif eval(values[1]) < minIncome + 2 * incomeCha:
        incomeLevel = 2
    elif eval(values[1]) < minIncome + 3 * incomeCha:
        incomeLevel = 3
    elif eval(values[1]) < minIncome + 4 * incomeCha:
        incomeLevel = 4
    else:
        incomeLevel = 5
    value.append(str(incomeLevel))
    maxIndustryIncome = socialAttributesTable_incomeBZ[socialAttributesTable_incomeBZ.loc[:, '行业名'] == values[2]].iloc[
        0, 1]
    minIndustryIncome = socialAttributesTable_incomeBZ[socialAttributesTable_incomeBZ.loc[:, '行业名'] == values[2]].iloc[
        0, 2]
    industryIncomeCha = (maxIndustryIncome - minIndustryIncome) / 3
    industryLevel = values[2]
    if eval(values[1]) < minIndustryIncome + industryIncomeCha:
        industryLevel += '新手'
    elif eval(values[1]) < minIndustryIncome + 2 * industryIncomeCha:
        industryLevel += '老手'
    else:
        industryLevel += '精英'
    value.append(industryLevel)
    educationLevel = 0
    if values[0] == "初中":
        educationLevel = 1
    elif values[0] == "高中":
        educationLevel = 2
    elif values[0] == "硕士":
        educationLevel = 4
    elif values[0] == "博士":
        educationLevel = 5
    else:
        educationLevel = 3
    value.append(str(educationLevel))
    socialAttributesTableBat.put(key,
                                 {'incLevel:0': value[0], 'indLevel:0': value[1], 'eduLevel:0': value[2]})

socialAttributesTableBat.send()
print("社会属性表 socialAttributesTable 完成！")
