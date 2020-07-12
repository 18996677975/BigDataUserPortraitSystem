import pandas as pd
import random
import happybase

"""数据准备"""
# 姓名
xing = ['陈', '林', '周', '张', '李', '黄', '王', '吴', '刘', '蔡', '杨', '许', '郑', '谢', '郭', '洪', '邱', '曾', ' 廖', '赖', '徐',
        '周', '叶', '苏', '庄', '江', '吕', '何', '罗', '高', '萧', '潘', '朱', '简', '锺', '彭', '游', '詹', '胡', '施', '沉', '余',
        '赵', '卢', '梁', '颜', '柯', '孙', '魏', '翁', '戴', '范', '宋', '方', '邓', '杜', '傅', '侯', '曹', '温', '薛', '丁', '马',
        '蒋', '唐', '卓', '蓝', '冯', '姚', '石', '董', '纪', '欧', '程', '连', '古', '汪', '汤', '姜', '田', '康', '邹', '白', '涂',
        '尤', '巫', '韩', '龚', '严', '袁', '钟', '黎', '金', '阮', '陆', '倪', '夏', '童', '邵', '柳', '钱']
zi = ['明', '国', '华', '建', '文', '平', '志', '伟', '东', '海', '强', '晓', '生', '光', '林', '小', '民', '永', '杰', '军', '波',
      '成', '荣', '新', '峰', '刚', '家', '龙', '德', '庆', '斌', '辉', '良', '玉', '俊', '立', '浩', '天', '宏', '子', '金', '健',
      '一', '忠', '洪', '江', '福', '祥', '中', '正', '振', '勇', '耀', '春', '大', '宁', '亮', '宇', '兴', '宝', '少', '剑', '云',
      '学', '仁', '涛', '瑞', '飞', '鹏', '安', '亚', '泽', '世', '汉', '达', '卫', '利', '胜', '敏', '群', '松', '克', '清', '长',
      '嘉', '红', '山', '贤', '阳', '乐', '锋', '智', '青', '跃', '元', '南', '武', '广', '思', '雄', '锦', '威', '启', '昌', '铭',
      '维', '义', '宗', '英', '凯', '鸿', '森', '超', '坚', '旭', '政', '传', '康', '继', '翔', '远', '力', '进', '泉', '茂', '毅',
      '富', '博', '霖', '顺', '信', '凡', '豪', '树', '和', '恩', '向', '道', '川', '彬', '柏', '磊', '敬', '书', '鸣', '芳', '培',
      '全', '炳', '基', '冠', '晖', '京', '欣', '廷', '哲', '保', '秋', '君', '劲', '栋', '仲', '权', '奇', '礼', '楠', '炜', '友',
      '年', '震', '鑫', '雷', '兵', '万', '星', '骏', '伦', '绍', '麟', '雨', '行', '才', '希', '彦', '兆', '贵', '源', '有', '景',
      '升', '惠', '臣', '慧', '开', '章', '润', '高', '佳', '虎', '根', '诚', '夫', '声', '冬', '奎', '扬', '双', '坤', '镇', '楚',
      '水', '铁', '喜', '之', '迪', '泰', '方', '同', '滨', '邦', '先', '聪', '朝', '善', '非', '恒', '晋', '汝', '丹', '为', '晨',
      '乃', '秀', '岩', '辰', '洋', '然', '厚', '灿', '卓', '轩', '帆', '若', '连', '勋', '祖', '锡', '吉', '崇', '钧', '田', '石',
      '奕', '发', '洲', '彪', '钢', '运', '伯', '满', '庭', '申', '湘', '皓', '承', '梓', '雪', '孟', '其', '潮', '冰', '怀', '鲁',
      '裕', '翰', '征', '谦', '航', '士', '尧', '标', '洁', '城', '寿', '枫', '革', '纯', '风', '化', '逸', '腾', '岳', '银', '鹤',
      '琳', '显', '焕', '来', '心', '凤', '睿', '勤', '延', '凌', '昊', '西', '羽', '百', '捷', '定', '琦', '圣', '佩', '麒', '虹',
      '如', '靖', '日', '咏', '会', '久', '昕', '黎', '桂', '玮', '燕', '可', '越', '彤', '雁', '孝', '宪', '萌', '颖', '艺', '夏',
      '桐', '月', '瑜', '沛', '杨', '钰', '兰', '怡', '灵', '淇', '美', '琪', '亦', '晶', '舒', '菁', '真', '涵', '爽', '雅', '爱',
      '依', '静', '棋', '宜', '男', '蔚', '芝', '菲', '露', '娜', '珊', '雯', '淑', '曼', '萍', '珠', '诗', '璇', '琴', '素', '梅',
      '玲', '蕾', '艳', '紫', '珍', '丽', '仪', '梦', '倩', '伊', '茜', '妍', '碧', '芬', '儿', '岚', '婷', '菊', '妮', '媛', '莲',
      '娟']
# 城市
citys = ['北京', '天津', '上海', '重庆', '新疆', '西藏', '宁夏', '内蒙古', '广西', '黑龙江', '吉林', '辽宁', '河北', '山东', '江苏',
         '安徽', '浙江', '福建', '广东', '海南', '云南', '贵州', '四川', '湖南', '湖北', '河南', '山西', '陕西', '甘肃', '青海',
         '江西', '台湾', '香港', '澳门']
# 手机号前三位
phoneNum1_3 = ["130", "131", "132", "133", "134", "135", "136", "137", "138", "139", "147", "150", "151", "152", "153",
               "155", "156", "157", "158", "159", "186", "187", "188", "189"]
# 行业
industry = ["建筑业", "计算机业", "计算机业", "建筑业", "设计业", "化学业", "土木业", "销售业", "建筑业", "管理业", "医疗业", "医疗业",
            "医疗业", "医疗业", "医疗业", "教育业", "教育业", "会计业", "公共关系业", "媒体业", "媒体业", "媒体业", "制造业", "制造业",
            "建筑业", "管理业", "计算机业", "管理业", "教育业", "计算机业", "制造业", "设计业", "设计业", "计算机业", "媒体业", "运输业",
            "销售业", "金融业", "会计业", "设计业", "媒体业", "计算机业", "媒体业", "广告业", "会计业", "运输业", "运输业", "旅游业",
            "服务业", "旅游业", "餐饮业", "餐饮业", "餐饮业", "服务业", "服务业", "服务业", "销售业", "服务业", "建筑业", "建筑业",
            "汽车业", "汽车业", "餐饮业", "餐饮业", "制造业", "服装业", "服装业", "服装业", "服装业", "服装业", "服装业", "制造业",
            "制造业", "制造业", "制造业", "制造业", "运输业", "制造业", "服务业"]
# 职业
occupation = ["土木营造监工", "电脑程式设计人员", "系统分析师", "建筑师", "交通规划师", "化学工程技术师", "土木工程师", "销售工程师",
              "工业工程师", "品质管制工程师", "药师", "公共卫生医师", "中医师", "护理佐理员", "护理师", "学前教育教师", "特殊教育教师",
              "会计师", "公共关系员", "新闻记者", "报刊杂志编辑人员", "编剧", "石雕工", "木雕工", "营建及工程管理", "钢结构设计与管理人员",
              "微电脑", "资讯管理师", "幼稚教育教师", "网页设计师", "制程工程师", "机械制图工", "建筑制图人员", "电子处理资料系统操作员",
              "摄影工作人员", "民航运输驾驶员", "推销员", "房地产经纪人", "会计员", "室内设计师", "播音员", "美工人员", "电视节目主持人",
              "广告AE", "会计师助理", "邮务士", "邮政工作人员", "导游人员", "空中服务员", "文物解说员", "厨师", "调酒员", "西餐厨师",
              "餐旅服务人员", "保姆", "美容理发师", "商品售货员", "宠物美容师", "建筑工", "水电工", "汽车修护工", "汽车电系工", "烘焙食品",
              "食品及饮料技师", "缝纫工", "织布工", "西装工", "国服缝制人员", "制鞋工", "服装设计与制作人员", "女装工", "塑胶模具制造工",
              "塑胶制品制造工", "橡胶制品制造工", "纸制品制造工", "乳制品制造工", "汽车驾驶员", "车床工", "清洁服务人员"]
# 行业职业字典
industry_occupation = dict(zip(occupation, industry))
# 学历
education = ["初中", "高中", "专科", "本科", "硕士", "博士"]
# 商品名
commodity = ["子俊男装潮牌ins超火的运动夹克男韩版宽松休闲外套青年学生上衣", "【多件多折】连帽外套男飞行服韩版修身夹克学生帅气纯色外套",
             "牛仔外套男潮牌2019秋季欧美街头个性印花上衣男原宿风ulzzang潮", "春季休闲西装外套男韩版修身潮流帅气单西男士正装格子小西服上衣",
             "新款春秋海宁真皮皮衣男士进口胎牛皮短款皮夹克帅气翻领外套薄款", "男士长袖T恤2020新款韩版潮流内搭春秋季上衣服打底衫潮牌卫衣男",
             "唐狮长袖t恤男2019秋冬新款白色棉圆领学生打底衫潮牌宽松上衣", "thom browne代购 TB短袖T恤彩条口袋男女同款休闲前短后长打底衫",
             "2020新款男士牛仔裤弹力修身小脚长裤韩版潮流潮牌百搭休闲裤子男", "牛仔裤男春季欧美潮牌直筒抽绳休闲束脚裤阔腿宽松潮流九分裤子男",
             "工装外套男装春季潮牌ins帅气潮流百搭上衣服学生宽松机能风夹克", "THESSNCE 分割设计 春季宽松舒适休闲飞行员夹克衫上衣外套男",
             "男士毛衣半高领修身中领春秋季圆领加厚新款针织衫潮流羊毛衫男潮", "子俊男装秋冬撞色拼接毛衣韩版宽松潮流ins慵懒风套头圆领针织衫",
             "毛衣男韩版秋冬季宽松潮流拼色个性网红外穿百搭套头学生针织线衫", "香港专柜2020春夏新款条纹帅气暗扣修身休闲青年百搭小脚西装裤男",
             "香港2020春季新款九分西装裤男修身小脚韩版潮商务免烫透气休闲裤", "绫致SELECTED思莱德男士纯色含绵羊毛合体商务正装西裤41916B510",
             "春季新款街头潮流假两件拼接条纹衬衫男长袖宽松潮牌衬衣痞帅外套", "SELECTED思莱德撞色条纹宽松商务休闲短袖衬衫男|419204543",
             "棒球部的暗恋对象 背带裙女夏季日系连衣裙短裙/我在收集熊和证据", "云地素2020春装新款复古镂空蕾丝两件套仙女连衣裙网纱刺绣初恋裙",
             "【馨雨法代】maje 20春夏 小香风格纹收腰连衣裙MFPRO00809", "MUMU法国 MAJE秦岚宋妍霏明星同款女装2020夏季小香风格子连衣裙",
             "micco日本 iena 印花短袖连衣裙 20040900800010", "欧洲站轻奢重工珍珠针织连衣裙2020新款气质赫本风A字裙小黑裙女",
             "MOUMOU2020春秋新款气质维多利亚复古初恋超仙碎花连衣裙过膝长裙", "2020夏季新品大码显瘦中长款t恤裙女短袖长款鱼尾连衣裙打底裙春",
             "[QZ292222MG]笑涵阁遮肉显瘦！对条英文印花 舒适透气T恤连衣裙", "欧美范春季新款羊绒大衣女中长款宽松毛衣外套针织开衫加厚外搭",
             "网红针织衫女2020春季新款黄色v领毛衣韩版宽松开衫外套短款上衣", "tb开衫女毛衣短款小外套V领秋冬长袖外搭修身羊毛针织毛线上衣潮",
             "网红牛仔外套女2019新款潮ins宽松韩版BF风牛仔衣廓形中长款春秋", "牛仔外套女春秋季2020年新款宽松韩版百搭bf风复古港味上衣潮ins",
             "chic春季美伢bf情侣国潮夹克ins宽松薄款刺绣运动棒球服外套男女", "MONA 西装外套女春秋韩版宽松设计感小众法式复古chic小西服",
             "忠犬小八梨形自制2020春季新品BF风百搭复古日系牛仔外套短学生女", "古着感复古磨白OVERSIZE宽松大码牛仔外套",
             "绿光正反两面穿格子外套女2020春装新款复古港味宽松学生夹克潮", "Seven4lee一对快乐奶白色筷子白色运动裤女夏宽松束脚休闲裤潮",
             u"金丝绒阔腿裤女高腰春季新款亮丝休闲裤大码显瘦百搭直筒拖地裤子", u"烈儿Lierkiss【女神裤】弹力魔术裤 高腰紧身显瘦小脚休闲裤9E66",
             "灰色运动裤女春夏宽松束脚哈伦裤垂感显瘦百搭休闲ins潮灯笼卫裤", "套装女春秋2020新款洋气两件套大码牛仔拼接时尚休闲宽松哈伦裤冬",
             "休闲西装套装女2020春秋新款韩版时尚显瘦职业小西服外套两件套潮", "钱夫人 chic网红小西装外套女2020年春秋装韩版宽松休闲西服套装",
             "职业装套装女秋冬新款气质西服正装大学生面试装工作服小西装工装", "香港潮牌秋季休闲开衫跑步运动服套装长袖宽松全棉卫衣两件套女",
             "欧洲站法式复古长款毛衣裙过膝长裙碎花连衣裙女春秋马甲套装裙子", "红色运动服套装女春装2020新款春秋韩版潮宽松时尚卫衣休闲两件套",
             u"刀锋鞋男鞋春季2020新款网面跑步白鞋休闲百搭夏季透气运动鞋潮鞋", u"CONVERSE匡威官方 Chuck 70 经典低帮复古帆布鞋 162062C",
             "VANS范斯经典低帮黑白OS帆布鞋板鞋男鞋女鞋VN000D3HY28", u"美特斯邦威低帮休闲鞋男2020年新款春季潮流帅气搭配基本男士跑鞋",
             u"Bershka男鞋2020新款内增高厚底运动小白鞋老爹鞋男12330560001",
             u"季中折扣 Massimo Dutti男鞋 2020春季男士英伦风绒面真皮休闲鞋2203550085",
             u"李宁羽毛球鞋男鞋新款耐磨防滑支撑男士低帮春季季运动鞋AYTM031", u"李宁羽毛球鞋男鞋情侣鞋男士鞋子专业防滑低帮运动鞋",
             u"花花公子男鞋2020春季男士潮鞋低帮休闲帆布鞋男百搭高帮涂鸦板鞋", u"特步男鞋帆布鞋2020春季新品运动鞋韩版板鞋纯色时尚潮流休闲鞋男",
             "【sheii苏茵茵】大幂幂同款！徐璐ann马丁靴真皮系带机车靴女短靴", "帅气马丁靴女2019新款英伦风靴子女秋季百搭透气机车黑色短靴潮LY",
             u"代购2019秋冬真皮高跟马丁靴女英伦风粗跟短靴冬季黑色中筒单靴子", "keds亮片婚礼鞋小白鞋帆布鞋女鞋代购kate spade郑秀晶同款甜美",
             "香港代购老爹鞋女2020新款韩版春季旅游百搭休闲运动小白鞋女鞋子", "潮牌2020新款ins运动鞋女真皮老爹鞋韩版ulzzang原宿百搭小白鞋女",
             "真皮小白鞋女2020新款女鞋百搭平底春季爆款ins潮透气浅口板鞋nx", "法国小众设计师daymare vulcanization黑白红溶化涂鸦高帮帆布鞋",
             "加绒高帮女鞋2019新款冬季真皮爆款秋鞋秋冬网红潮鞋百搭鞋子女LY", "欧洲站2020春款厚底帆布鞋女学生韩版增高松糕高帮百搭超火休闲鞋",
             "真皮小白鞋女2019秋款爆款百搭冬季女鞋洋气高帮鞋秋季新款秋鞋LY", "真皮小白鞋女2019秋款高帮秋鞋加绒百搭潮鞋冬季板鞋新款女鞋冬",
             "PALLADIUM帕拉丁高帮帆布鞋女糖果色休闲鞋女鞋潮鞋新品96205", "真皮小白鞋女2019冬款平底百搭爆款加绒板鞋冬季高帮鞋潮女鞋皮面",
             "代购2019秋冬新款百搭银色高跟鞋女细跟伴娘尖头婚纱性感婚鞋单鞋", u"高跟女春秋2019新款一字扣绑带裸色百搭尖头细跟漆皮网红性感单鞋",
             u"高跟鞋女2020春季新款小香风气质浅口尖头细跟中跟百搭真皮单鞋子", "2020新款春秋银色高跟鞋女尖头细跟性感ins仙风网红漆皮单鞋羊皮",
             "2020春秋款亮片尖头高跟鞋女ins仙风细跟性感网红水晶鞋百搭单鞋", "法式小高跟鞋女细跟2020年新款春款百搭性感小清新尖头一字扣单鞋",
             "云鲸拖地机器人拖扫洗拖布一体智能扫地机拖地机智能超薄免洗拖", "扫地机器人家用全自动擦地拖地三合一体机智能吸尘神器超薄赠礼品",
             "志高烘干机家用速干衣小型滚筒式10KG大型容量商用杀菌除螨可壁挂", "【赵雅芝张铁林代言】易开得净水器 家用直饮 厨房台上式净水机器",
             "自动上水电热水壶水晶玻璃抽水自吸式家用智能烧水壶茶炉底部上水", "Intel/英特尔 i9-10940X 3.3G 14核28线程 酷睿i9盒装CPU处理器",
             "肥猫家の店 映众 RTX2060 6G 冰龙超级版 电竞游戏独立显卡 RGB", "顺丰赛睿Apex 7 电竞USB背光电脑游戏专用吃鸡神器可调节机械键盘",
             "Asus/华硕 X570 C8I X470 B450I主板套装技嘉36/37/39/00/50/XCPU",
             "AOC CQ27G2 27英寸2K曲面电竞144HZ吃鸡1ms响应游戏可壁挂显示器", "2020年全新全栈后端高级工程师面试视频教程面试宝典新版java面试",
             u"机器学习-决策树&集成学习 Python人工智能 入门 进阶 全栈开发", u"vb视频教程 编程 vb教程视频零基础教学视频程序设计从入门到精通",
             "中学生c++编程信息学奥赛一本通网课青少年C十十竞赛NOIP视频教程", u"CAD二次开发教程 c#版 c#CAD二次开发视频 CAD编程c#学习视频",
             "Lua编程实战视频教程淘宝教育在线观看Unity开始进阶实战", "web前端工程师零基础到就业全套课程(站内学习版)",
             "mvvm,web前端javascript框架knockoutjs 3.4.0在线视频课程", "西门子PLC博途SCL高级语言编程视频教程 正版原创",
             u"\人生必读十本书 人性的弱点卡耐基正版+墨菲定律狼道鬼谷子正版书+羊皮卷厚黑学口才受益一生书励志书籍",
             "口才三绝正版全套 为人三会 套装修心三不 3本如何提升提高会说话技巧的书学会沟通术演讲与休心训练人际交往高情商书籍畅销书三册",
             "受益一生的五本书 鬼谷子墨菲定律狼道书籍正版+人性的弱点书卡耐基+羊皮卷正版书为人处世成功励志书籍",
             "新华书店追风筝的人(全2册)中英文对照版现当代文学", "钢铁是怎样炼成的",
             "哈利波特与死亡圣器/书正版包邮初中（7年级下阅读）纪念版哈利波特第七部/魔法小说儿童文学/教育推 荐书目/人民文学出版社",
             "好欢螺螺蛳粉300g*6袋柳州特产螺狮粉美食螺丝粉煮水方便面酸辣粉", "【良品铺子-肉松饼380gx2袋】传统糕点早餐食品零食美食小吃休闲",
             "【良品铺子-虎皮凤爪200g】卤味鸡爪美食小吃零食熟食", u"港荣奶香蒸蛋糕点点心 整箱营养早餐零食品美食手撕小吃面包邮",
             "知味观东坡肉杭州特产卤肉真空加热即食猪肉红烧肉小吃下酒菜熟食", "百草味肉松饼1kg早餐蛋糕饼干糕点网红休闲零食小吃整箱特色美食",
             "科尔沁手撕风干牛肉干400g原味内蒙古特产牛肉干美食零食小吃", u"轩妈家蛋黄酥2盒 红豆味雪媚娘麻薯新鲜糕点美食网红零食小吃早餐",
             "\阿宽白家红油面皮12袋1260g特色小吃方便面非火鸡面成都特色美食", "轩妈家蛋黄酥4种口味组合混合装糕点点心美食零食小吃早餐食品",
             "良品铺子原味猪肉脯200g零食小吃靖江猪肉干休闲食品美食小包装", "【周黑鸭旗舰店_锁鲜】气调盒装卤鸭舌150g*2 美食特产零食品小吃",
             "沈大成糕点青芝士团网红零食小吃老字号美食上海特产", "李村猪肉脂渣香酥油渣干炸五花肉粕青岛特产小吃美食肉类生酮零食",
             "桂花鸭南京盐水鸭1000g正宗江苏特产年货美食板鸭咸水鸭真空熟食", u"\满减【三只松鼠_墨玉川式芝麻酥135g】美食早餐小吃点心休闲零食",
             "沈师傅鸡蛋干四川特产豆腐干成都美食豆干小吃零食休闲食品非150g", "桂花鸭南京盐水鸭+酱鸭2000g特产正宗美食送礼真空装熟食年货食品",
             "好丽友呀土豆9连包超值礼包实惠家庭装休闲膨化薯片饼干美食小吃", "稻香村芝麻瓦片450g好吃传统糕点点心饼干休闲零食品美食特产小吃",
             "湖南特产常德长沙正宗手撕酱板鸭香辣风干烤鸭美食熟即食零食小吃", "【德州扒鸡旗舰店】德州扒鸡老字号卤零食美食零食扒小鸡300g*2只",
             "【卫龙旗舰店】小面筋锁鲜装32g*40包香辣条零食经典90后怀旧美食", "食家巷甘肃特产白吉饼白吉馍百吉饼白饼子烤饼馍馍电烙烧饼20个装",
             "蜀道难自热火锅米饭 懒人速食烧烤方便拌饭即食品自加热快餐重庆", "秦之恋襄阳手工锅巴400g*4麻辣味老襄阳特产小吃美食休闲网红零食",
             u"\满299减200丨麻辣鸡翅尖100g四川特产美食休闲网红鸡爪鸡翅零食", "凉皮速食擀面皮真空袋装陕西特产宝鸡西安岐山汉中小吃名吃美食干",
             "轩妈家蛋黄酥20g*8枚 红豆味迷你mini儿童零食糕点点心网红美食", "老街口 焦糖/山核桃味瓜子500g*4袋装葵花籽坚果炒货零食品批发"]
# 商品价格
price = [179, 339, 108, 479, 480, 338, 79, 500, 379, 138, 179, 469, 368, 98, 108, 499, 368, 359, 128, 239, 152,
         256, 420, 750, 500, 418, 388, 265, 248, 280, 189, 148, 126, 89, 165, 389, 119, 105, 148, 140, 129, 99,
         88, 198, 158, 296, 250, 375, 398, 239, 158, 539, 429, 339, 429, 690, 492, 499, 298, 209, 487, 388, 585,
         433, 375, 328, 319, 218, 468, 268, 358, 368, 408, 388, 368, 398, 448, 569, 569, 356, 3699, 1299, 1399,
         1980, 908, 8000, 2300, 1400, 1679, 1899, 198, 99, 55, 399, 298, 36, 399, 56, 176, 88, 30, 68, 90, 40, 56,
         70, 30, 26, 70, 50, 40, 125, 89, 42, 66, 30, 120, 50, 118, 56, 20, 37, 109, 40, 30, 40, 80, 47, 25, 22,
         23, 29, 29, 30, 40]
# 商品类型
typesOf = ["衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服",
           "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服",
           "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服", "衣服",
           "衣服", "衣服", "衣服", "衣服", "衣服", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子",
           "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "鞋子",
           "鞋子", "鞋子", "鞋子", "鞋子", "鞋子", "家电", "家电", "家电", "家电", "家电", "电子产品", "电子产品", "电子产品", "电子产品",
           "电子产品", "学习", "学习", "学习", "学习", "学习", "学习", "学习", "学习", "学习", "学习", "学习", "学习", "学习", "学习",
           "学习", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品",
           "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品", "食品"]

saleTableTimes = [i for i in range(20190801, 20190832)] + [i for i in range(20190901, 20190932)] + \
                 [i for i in range(20191001, 20191009)]
internetTableTimes = [i for i in range(20190801, 20190832)] + [i for i in range(20190901, 20190932)] + \
                     [i for i in range(20191001, 20191009)]

incomeBZ = pd.read_csv("data/各省人均收入表.csv")

sexBZ = ["男", "男", "男", "女", "女"]

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

"""数组初始化"""
phones = []
basicFeaturesTableNum = 0
socialAttributesTableNum = 0
consumptionCharacteristicsTableNum = 0
internetBehaviorTableNum = 0

"""循环创建数据并存入HBASE"""
for i in range(200000):
    phone = random.choice(phoneNum1_3) + "".join(random.choice("0123456789") for i in range(8))
    if phone in phones:
        continue
    else:
        phones.append(phone)
    xing_index = random.randint(0, len(xing) - 1)
    name = xing[xing_index]
    two_or_three = random.randint(1, 2)
    for j in range(two_or_three):
        zi_index = random.randint(0, len(zi) - 1)
        name += zi[zi_index]
    sex = random.choice(sexBZ)
    age = random.randint(15, 60)
    height = random.randint(155, 200)
    wight = random.randint(40, 80)
    place = citys[random.randint(0, len(citys) - 1)]
    basicFeaturesTableBat.put("%s" % phone, {'name:0': name, 'sex:0': sex, 'age:0': "%s" % age,
                                             'height:0': "%s" % height, 'wight:0': "%s" % wight, 'place:0': place})
    basicFeaturesTableNum += 1
    tempOccupation = random.choice(occupation)
    tempIndustry = industry_occupation.get(tempOccupation)
    tempEducation = random.choice(education)
    for j in range(incomeBZ.shape[0]):
        if place == incomeBZ.iloc[j, 0]:
            income = random.randint(incomeBZ.iloc[j, 1] - 35000, incomeBZ.iloc[j, 1] + 35000)
    if tempEducation == "初中":
        income = income * 0.75
    elif tempEducation == "高中":
        income = income * 0.85
    elif tempEducation == "专科":
        income = income
    elif tempEducation == "本科":
        income = income * 1.15
    elif tempEducation == "硕士":
        income = income * 1.25
    else:
        income = income * 1.5
    socialAttributesTableBat.put("%s" % phone, {'industry:0': tempIndustry, 'occupation:0': tempOccupation,
                                                'education:0': tempEducation, 'income:0': "%s" % income})
    socialAttributesTableNum += 1
    saleTimes = random.randint(5, 50)
    for j in range(saleTimes):
        saleTableName = name
        tempNum = random.randint(0, len(commodity) - 1)
        tempCommodity = commodity[tempNum]
        tempPrice = price[tempNum]
        tempType = typesOf[tempNum]
        saleTableTime = random.choice(saleTableTimes)
        consumptionCharacteristicsTableBat.put("%s" % phone,
                                               {'commodity:%s' % j: tempCommodity, 'type:%s' % j: tempType,
                                                'price:%s' % j: "%s" % tempPrice, 'date:%s' % j: "%s" % saleTableTime})
        consumptionCharacteristicsTableNum += 1
    for j in range(70):
        internetTableName = name
        newsTime = random.randint(0, 4)
        communicationTime = random.randint(0, 4)
        entertainmentTime = random.randint(0, 4)
        domesticServicesTime = random.randint(0, 4)
        busAppTime = random.randint(0, 4)
        toolUseTime = random.randint(0, 4)
        internetTableTime = internetTableTimes[j]
        internetBehaviorTableBat.put("%s" % phone, {'news:%s' % j: "%s" % newsTime,
                                                    'communications:%s' % j: "%s" % communicationTime,
                                                    'entertainment:%s' % j: "%s" % entertainmentTime,
                                                    'domersticServices:%s' % j: "%s" % domesticServicesTime,
                                                    'busApp:%s' % j: "%s" % busAppTime,
                                                    'toolUse:%s' % j: "%s" % toolUseTime,
                                                    'date:%s' % j: "%s" % internetTableTime})
        internetBehaviorTableNum += 1

basicFeaturesTableBat.send()
socialAttributesTableBat.send()
consumptionCharacteristicsTableBat.send()
internetBehaviorTableBat.send()

# 断开连接
connection.close()

print("数据保存完毕!")
print("基础属性表 basicFeaturesTable 共 " + str(basicFeaturesTableNum) + " 条数据")
print("社会属性表 socialAttributesTable 共 " + str(socialAttributesTableNum) + " 条数据")
print("消费情况表 consumptionCharacteristicsTable 共 " + str(consumptionCharacteristicsTableNum) + " 条数据")
print("互联网行为表 internetBehaviorTable 共 " + str(internetBehaviorTableNum) + " 条数据")
