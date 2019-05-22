from pyspark import SparkContext
import os
cwd = os.getcwd()

# 导入Spark上下文
sc = SparkContext("local", "movielens")
# 初始化Spark上下文，指定master为local，即本地运行，应用名称为movielens
user_data = sc.textFile("file:\\\\\\" + cwd + "\\ml-100k\\u.user")
# 加载本地movielens文件中的用户信息文件，file://开头，后接本地文件路径；也可上传至HDFS,hdfs://192.168.1.101:9000/ml-100k/u.user
print(user_data.first())
# 输出第一行  测试23567

# u'1|24|M|technician|85711'
# 用户信息文件包含	用户ID|年龄|性别|职业|邮编

user_fields = user_data.map(lambda line: line.split("|"))
# 将用户信息文件的每一行以|为分隔符【分开】
num_users = user_fields.map(lambda fields: fields[0]).count()
# 将用户信息文件的用户ID列取出，并且【计算总数】，得到用户数目
num_genders = user_fields.map(lambda fields: fields[2]).distinct().count()
# 将用户信息文件的性别列取出，并进行【去重】，并且计算总数，得到性别数目
num_occupations = user_fields.map(lambda fields: fields[3]).distinct().count()
# 将用户信息文件的职业列取出，并进行去重，并且计算总数，得到职业数目
num_zipcodes = user_fields.map(lambda fields: fields[4]).distinct().count()
print("Users: %d, genders: %d, occupations: %d, ZIP codes: %d" % (num_users, num_genders, num_occupations, num_zipcodes))
# 输出上述信息
# Users: 943, genders: 2, occupations: 21, ZIP codes: 795

ages = user_fields.map(lambda x: int(x[1])).collect()
# 将用户信息文件的年龄列取出
import matplotlib.pyplot

# 导入pyplot库
matplotlib.pyplot.hist(ages, bins=20, color='lightblue', normed=True)
# 画直方图，参数列表如下

"""
matplotlib.pyplot.hist(x, bins=None, range=None, normed=False, weights=None, cumulative=False, bottom=None, histtype=’bar’, align=’mid’, orientation=’vertical’, rwidth=None, log=False, color=None, label=None, stacked=False, hold=None, data=None, **kwargs)
Parameters:
x : (n,) array or sequence of (n,) arrays(可以是一个array也可以是多个array)
integer or array_like or ‘auto’, optional(可以是整型来设置箱子的宽度,也可以是array,指定每个箱子的宽度)
range : tuple or None, optional(设置显示的范围,范围之外的将被舍弃)
normed : boolean, optional(?)
weights : (n, ) array_like or None, optional(?)
cumulative : boolean, optional(?)
bottom : array_like, scalar, or None(?)
histtype : {‘bar’, ‘barstacked’, ‘step’, ‘stepfilled’}, optional(选择展示的类型,默认为bar)
align : {‘left’, ‘mid’, ‘right’}, optional(对齐方式)
orientation : {‘horizontal’, ‘vertical’}, optional(箱子方向)
log : boolean, optional(log刻度)
color : color or array_like of colors or None, optional(颜色设置)
label : string or None, optional(刻度标签)
stacked : boolean, optional(?)
return
n : array or list of arrays(箱子的值)
bins : array(箱子的边界)
patches : list or list of lists
"""

fig = matplotlib.pyplot.gcf()
# 得到一个当前画图的引用
fig.set_size_inches(16, 10)

"""
fig.set_size_inches(w,h,forward=False)
atplotlib 包中提供的函数，用于设置图形的尺寸，单位为英寸。1英寸等于 2.54 cm。
参数forward = True表示自动更新画布大小。
"""
matplotlib.pyplot.show()
# 显示
