import happybase
import pandas as pd

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()

"""最高最低收入与每个行业的收入最高最低值"""
name = []
maxNum = []
minNum = []

socialAttributesTable = happybase.Table('socialAttributesTable', connection)

# 最高最低收入
socialAttributesTable_Income_Scanner = socialAttributesTable.scan(columns=["income"])
income = []
for k, v in socialAttributesTable_Income_Scanner:
    values = list(v.values())
    values = list(map(lambda x: x.decode(), values))
    values = eval(values[0])
    income.append(values)

name.append("总收入")
maxNum.append(round(max(income)))
minNum.append(round(min(income)))

# 求出所有的行业
socialAttributesTable_Industry_Scanner = socialAttributesTable.scan(columns=["industry"])
industry = []
for k, v in socialAttributesTable_Industry_Scanner:
    value = list(v.values())
    value = list(map(lambda x: x.decode(), value))
    value = value[0]
    if value in industry:
        continue
    industry.append(value)
    name.append(value)

# 每个行业的最高最低收入
for i in range(len(industry)):
    filterDemo = "ValueFilter(=, 'substring:%s')" % industry[i]
    rowsNum = []
    socialAttributesTable_Industry_Scanner = socialAttributesTable.scan(filter=filterDemo)
    for k, v in socialAttributesTable_Industry_Scanner:
        rowsNum.append(k)
    info = socialAttributesTable.rows(rowsNum, columns=["income"])
    tempData = []
    for j in info:
        value = list(j[1].values())
        value = eval(value[0].decode())
        tempData.append(value)
    maxNum.append(round(max(tempData)))
    minNum.append(round(min(tempData)))

data = pd.DataFrame([name, maxNum, minNum]).T
data.columns = ["行业名", "最高收入", "最低收入"]
data.to_csv("data/各行业最高最低收入表.csv", index=None)
