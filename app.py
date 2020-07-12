from flask import *
import csv
from datetime import timedelta
import people_plt
import happybase
import random

"""连接hbase"""
connection = happybase.Connection(host="localhost", port=9090)
"""打开传输"""
connection.open()
"""连接表"""
basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
socialAttributesTable = happybase.Table('socialAttributesTable', connection)

# 初始化 app
app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = timedelta(seconds=1)  # 修改缓存时间，秒做单位
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(seconds=1)  # 修改回话存活时间，秒做单位
app.secret_key = '+\xf8\x1ck\xedM\x1a\xe5\x99Qr\xa8\x85P\x93lI\x88\x1c[\x98"\tC'

zongRowsNum = []
selectData = []
nowCity = ['重庆']


# 首页
@app.route('/')
def home_page():
    return render_template("page 1.html")


# 城市概况
@app.route('/city_display', methods=['GET', 'POST'])
def city_display():
    # if request.method == 'POST':
    #     session['city'] = request.form.get('city_select')
    # city = session['city']     # 城市名称
    city = nowCity[0]
    # print(city)
    # 读取数据
    with open("static/data/各省用户特征数据.csv", 'r', encoding='utf-8') as f:
        reader = csv.reader(f)  # 读取列表
        fieldnames = next(reader)  # 获取数据的第一行，作为后续要转为字典的键名 生成器，next方法获取
        csv_reader = csv.DictReader(f, fieldnames=fieldnames)  # 以list的形式存放键名
        for row in csv_reader:
            key = list(row.keys())[0]
            data = {}
            if (row[key]) == city:
                for k, v in row.items():
                    data[k] = v
                break
        data["age"] = eval(data["age"])
        data["edu"] = eval(data["edu"])
        data["job"] = eval(data["job"])
        data["sur_time"] = eval(data["sur_time"])
    return render_template('page 3.html', city_data=data)


# 城市消费特征
@app.route('/Preference', methods=['GET', 'POST'])
def Preference():
    # if request.method == 'POST':
    #     session['city'] = request.form.get('city_select')
    # city = session['city']     # 城市名称
    city = nowCity[0]
    # 读取数据
    with open("static/data/各省消费特征数据.csv", 'r', encoding='utf-8') as f:
        reader = csv.reader(f)  # 读取列表
        fieldnames = next(reader)  # 获取数据的第一行，作为后续要转为字典的键名 生成器，next方法获取
        csv_reader = csv.DictReader(f, fieldnames=fieldnames)  # 以list的形式存放键名
        for row in csv_reader:
            key = list(row.keys())[0]
            data = {}
            if (row[key]) == city:
                for k, v in row.items():
                    data[k] = v
                break

    return render_template('page 4.html', data=data)


# 数据预览
@app.route('/select', methods=['GET', 'POST'])
def select():
    # 连接hbase
    connection = happybase.Connection(host="localhost", port=9090)
    # 打开传输
    connection.open()
    # 连接表
    basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
    socialAttributesTable = happybase.Table('socialAttributesTable', connection)

    try:
        if request.method == 'POST':
            session['city'] = request.form.get('city_select')
        city = session['city']
        if len(nowCity) == 0:
            nowCity.append(city)
        else:
            nowCity.clear()
            nowCity.append(city)
    except:
        city = nowCity[0]

    data = []

    # 读取数据
    with open("static/data/城市每个人的key.csv", 'r', encoding="utf-8") as f:
        reader = csv.reader(f)  # 读取列表
        fieldnames = next(reader)  # 获取数据的第一行，作为后续要转为字典的键名 生成器，next方法获取
        csv_reader = csv.DictReader(f, fieldnames=fieldnames)  # 以list的形式存放键名
        for row in csv_reader:
            key = list(row.keys())[0]
            d = {}
            if (row[key]) == city:
                for k, v in row.items():
                    d[k] = v
                break
    rowsNum = eval(d["keys"])

    # 将当前城市所以人的key加入zongRowsNum
    if len(zongRowsNum) == 0:
        zongRowsNum.append(rowsNum)
    else:
        zongRowsNum.clear()
        zongRowsNum.append(rowsNum)

    # 随机选取50条
    rowsNum = random.choices(rowsNum, k=50)

    # 数据传入前端
    for i in rowsNum:
        try:
            bInfo = basicFeaturesTable.row(i, columns=["name", "sex", "age"])
            sInfo = socialAttributesTable.row(i, columns=["occupation", "income", "education"])
        except:
            # 连接hbase
            connection = happybase.Connection(host="localhost", port=9090)
            # 打开传输
            connection.open()
            # 连接表
            basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
            socialAttributesTable = happybase.Table('socialAttributesTable', connection)

            bInfo = basicFeaturesTable.row(i, columns=["name", "sex", "age"])
            sInfo = socialAttributesTable.row(i, columns=["occupation", "income", "education"])

        bValue = bInfo.values()
        bValue = list(map(lambda x: x.decode(), bValue))
        sValue = sInfo.values()
        sValue = list(map(lambda x: x.decode(), sValue))
        value = bValue + sValue
        value.append(city)
        value.append(i.decode())
        value = [value[6], value[1], value[2], value[0], value[5], value[4], value[3], value[7]]
        value[5] = str(round(eval(value[5])))
        data.append(value)

    if len(selectData) == 0:
        selectData.append(data)
    else:
        selectData.clear()
        selectData.append(data)

    return render_template('page 2.html', data=data, city=city, num=len(data))


# 数据查找
@app.route('/ones_select', methods=['GET', 'POST'])
def ones_select():
    # 连接hbase
    connection = happybase.Connection(host="localhost", port=9090)
    # 打开传输
    connection.open()
    # 连接表
    basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
    socialAttributesTable = happybase.Table('socialAttributesTable', connection)

    if request.method == 'POST':
        session['name'] = request.form.get('name')
    name = session['name']

    city = nowCity[0]

    if name == "":
        return render_template('page 2.html', city=city, data=selectData[0], num=50)

    user_data = []  # 存放个人信息
    rowsNum = []

    for i in zongRowsNum[0]:
        try:
            value = list(basicFeaturesTable.row(i, columns=["name"]).values())[0].decode()
        except:
            # 连接hbase
            connection = happybase.Connection(host="localhost", port=9090)
            # 打开传输
            connection.open()
            # 连接表
            basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
            value = list(basicFeaturesTable.row(i, columns=["name"]).values())[0].decode()

        if name in value:
            rowsNum.append(i)

    if len(rowsNum) != 0:
        for i in rowsNum:
            try:
                bInfo = basicFeaturesTable.row(i, columns=["name", "sex", "age"])
                sInfo = socialAttributesTable.row(i, columns=["occupation", "income", "education"])
            except:
                # 连接hbase
                connection = happybase.Connection(host="localhost", port=9090)
                # 打开传输
                connection.open()
                # 连接表
                basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
                socialAttributesTable = happybase.Table('socialAttributesTable', connection)

                bInfo = basicFeaturesTable.row(i, columns=["name", "sex", "age"])
                sInfo = socialAttributesTable.row(i, columns=["occupation", "income", "education"])

            bValue = bInfo.values()
            bValue = list(map(lambda x: x.decode(), bValue))
            sValue = sInfo.values()
            sValue = list(map(lambda x: x.decode(), sValue))
            value = bValue + sValue
            value.append(city)
            value.append(i.decode())
            value = [value[6], value[1], value[2], value[0], value[5], value[4], value[3], value[7]]
            value[5] = str(round(eval(value[5])))
            user_data.append(value)
        return render_template('page 2.html', city=city, data=user_data, num=len(user_data))
    else:
        user_data = selectData[0]
        return render_template('page 2.html', city=city, data=user_data, num=0)


# 个人画像页面
@app.route('/per_portrait', methods=['POST', 'GET'])
def per_portrait():
    # 连接hbase
    connection = happybase.Connection(host="localhost", port=9090)
    # 打开传输
    connection.open()
    # 连接表
    basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
    socialAttributesTable = happybase.Table('socialAttributesTable', connection)
    consumptionFeaturesTable = happybase.Table('consumptionFeaturesTable', connection)
    internetFeaturesTable = happybase.Table('internetFeaturesTable', connection)

    info = []
    if request.method == 'POST':
        session['unique'] = request.form.get('search')  # 唯一标识符
    key = session['unique']

    try:
        bKeys = basicFeaturesTable.row(key).keys()
        sKeys = socialAttributesTable.row(key).keys()
        cKeys = consumptionFeaturesTable.row(key).keys()
        iKeys = internetFeaturesTable.row(key).keys()
    except:
        # 连接hbase
        connection = happybase.Connection(host="localhost", port=9090)
        # 打开传输
        connection.open()
        # 连接表
        basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
        socialAttributesTable = happybase.Table('socialAttributesTable', connection)
        consumptionFeaturesTable = happybase.Table('consumptionFeaturesTable', connection)
        internetFeaturesTable = happybase.Table('internetFeaturesTable', connection)
        bKeys = basicFeaturesTable.row(key).keys()
        sKeys = socialAttributesTable.row(key).keys()
        cKeys = consumptionFeaturesTable.row(key).keys()
        iKeys = internetFeaturesTable.row(key).keys()

    bKeys = list(map(lambda x: x.decode(), bKeys))
    bKeys = list(map(lambda x: x[:x.index(":")], bKeys))
    sKeys = list(map(lambda x: x.decode(), sKeys))
    sKeys = list(map(lambda x: x[:x.index(":")], sKeys))
    cKeys = list(map(lambda x: x.decode(), cKeys))
    cKeys = list(map(lambda x: x[:x.index(":")], cKeys))
    iKeys = list(map(lambda x: x.decode(), iKeys))
    iKeys = list(map(lambda x: x[:x.index(":")], iKeys))
    keys = bKeys + sKeys + cKeys + iKeys

    try:
        bValue = basicFeaturesTable.row(key).values()
        sValue = socialAttributesTable.row(key).values()
        cValue = consumptionFeaturesTable.row(key).values()
        iValue = internetFeaturesTable.row(key).values()
    except:
        # 连接hbase
        connection = happybase.Connection(host="localhost", port=9090)
        # 打开传输
        connection.open()
        # 连接表
        basicFeaturesTable = happybase.Table('basicFeaturesTable', connection)
        socialAttributesTable = happybase.Table('socialAttributesTable', connection)
        consumptionFeaturesTable = happybase.Table('consumptionFeaturesTable', connection)
        internetFeaturesTable = happybase.Table('internetFeaturesTable', connection)
        bValue = basicFeaturesTable.row(key).values()
        sValue = socialAttributesTable.row(key).values()
        cValue = consumptionFeaturesTable.row(key).values()
        iValue = internetFeaturesTable.row(key).values()

    bValue = list(map(lambda x: x.decode(), bValue))
    sValue = list(map(lambda x: x.decode(), sValue))
    cValue = list(map(lambda x: x.decode(), cValue))
    iValue = list(map(lambda x: x.decode(), iValue))
    value = bValue + sValue + cValue + iValue

    # 将 互联网主要用途栏 数据，从英文改为中文
    internetBehaviorE = ["news", "communications", "entertainment", "domersticServices", "busApp", "toolUse"]
    internetBehaviorZ = ["新闻资讯", "通信交流", "娱乐休闲", "生活服务", "商务应用", "工具使用"]
    for i in range(6):
        if value[-5] == internetBehaviorE[i]:
            value[-5] = internetBehaviorZ[i]
        if value[-3] == internetBehaviorE[i]:
            value[-3] = internetBehaviorZ[i]

    data = dict(zip(keys, value))
    info.append(data)

    people_plt.plt(key)
    return render_template('page 5.html', info=info)


if __name__ == '__main__':
    app.run(debug=True)
