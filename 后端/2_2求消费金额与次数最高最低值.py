import happybase
import pandas as pd

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()

"""最高最低消费金额与次数"""
# 读取数据
consumptionCharacteristicsTable = happybase.Table('consumptionCharacteristicsTable', connection)

# 消费金额、消费次数
money = []
times = []
j = 0
scanner = consumptionCharacteristicsTable.scan(columns=["price"])
for k, v in scanner:
    value = list(v.values())
    value = list(map(lambda x: x.decode(), value))
    value = list(map(int, value))
    money.append(sum(value))
    times.append(len(value))

max_min_money = []
max_min_times = []

max_min_money.append(round(max(money)))
max_min_money.append(round(min(money)))
max_min_times.append(round(max(times)))
max_min_times.append(round(min(times)))

data = pd.DataFrame([max_min_money, max_min_times]).T
data.columns = ["sumMoney", "times"]
data.to_excel("data/最高最低消费金额与次数.xlsx", index=None)
