import happybase

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()

"""连接表"""
basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
basicFeaturesTableBat = basicFeaturesTable.batch(batch_size=1000)

"""自然属性表 basicFeaturesTable 分析"""
basicFeaturesTableScanner = basicFeaturesTable.scan(columns=["wight", "height"])
for k, v in basicFeaturesTableScanner:
    key = k.decode()
    values = list(v.values())
    values = list(map(lambda x: x.decode(), values))
    bmi = eval(values[1]) / ((eval(values[0]) / 100) ** 2)
    if bmi < 18.5:
        bmi = '偏瘦'
    elif bmi < 24:
        bmi = '正常'
    elif bmi < 27:
        bmi = '偏胖'
    elif bmi < 30:
        bmi = '肥胖'
    else:
        bmi = '重度肥胖'
    basicFeaturesTableBat.put(key, {'bmi:0': bmi})

basicFeaturesTableBat.send()
print("基础属性表 basicFeaturesTable 完成！")
