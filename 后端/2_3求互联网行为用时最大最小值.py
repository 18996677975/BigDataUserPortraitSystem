import happybase
import pandas as pd

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()

"""最高最低互联网行为用时"""
# 读取数据
internetBehaviorTable = happybase.Table('internetBehaviorTable', connection)

# 互联网行为用时
internetTime = []

scanner = internetBehaviorTable.scan(columns=["news", "communications",
                                              "entertainment", "domersticServices",
                                              "busApp", "toolUse"])
for k, v in scanner:
    value = list(v.values())
    value = list(map(lambda x: x.decode(), value))
    value = list(map(int, value))
    internetTime.append(sum(value))

max_min_internetTime = []

max_min_internetTime.append(max(internetTime))
max_min_internetTime.append(min(internetTime))

data = pd.DataFrame([max_min_internetTime]).T
data.columns = ["internetTime"]
data.to_csv("data/最高最低互联网行为用时.csv", index=None)
