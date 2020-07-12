import happybase
import pandas as pd

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()

"""每个省的每个人的key"""
# 读取数据
basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)

place = ['北京', '天津', '上海', '重庆', '新疆', '西藏', '宁夏', '内蒙古', '广西', '黑龙江', '吉林', '辽宁', '河北', '山东', '江苏',
         '安徽', '浙江', '福建', '广东', '海南', '云南', '贵州', '四川', '湖南', '湖北', '河南', '山西', '陕西', '甘肃', '青海',
         '江西', '台湾', '香港', '澳门']
keyNum = []

for i in range(len(place)):
    filterDemo = "ValueFilter(=, 'substring:%s')" % place[i]
    rowsNum = []
    scanner = basicFeaturesTable.scan(filter=filterDemo)
    for k, v in scanner:
        rowsNum.append(k)
    keyNum.append(rowsNum)

data = pd.DataFrame([place, keyNum]).T
data.columns = ["place", "keys"]
data.to_csv("data/城市每个人的key.csv", index=None)
data.to_csv("../static/data/城市每个人的key.csv", index=None)
