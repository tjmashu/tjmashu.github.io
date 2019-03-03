---
layout: post
title:  "Pyspark技能记录：Dataframe基本操作"
categories: Pyspark
tags: 数据开发
author: MaShu
---

* content
{:toc}

数据分析师通常用Hive SQL进行操作，但其实可以考虑换成用Pyspark。Spark是基于内存计算的，比Hive快，PySpark SQL模块的很多函数和方法与Hive SQL中的关键字一样，Pyspark还可与Python中的模块结合使用。不过Pyspark Dataframe和Pandas DataFrame差别还是比较大，这里对Pyspark Dataframe的基本操作进行总结。
## 1. 连接本地spark
```py
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```
## 2.创建DataFrame
**用Spark SQL创建DataFrame:**
```py
sparkdf = spark.sql(""" select * from table_name """)
```
**手动创建DataFrame:**
```py
df = spark.createDataFrame([('0001','F','H',1), ('0002','M','M',0), ('0003','F','L',1),
                            ('0004','F','H',0), ('0005','M','M',1), ('0006','F','H',1)
                           ], ['userid','gender','level','vip'])
```
## 3.概况
#### 3.1 以树形式打印概要
`df.printSchema()`
```py
root
 |-- userid: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
 |-- vip: long (nullable = true)
```
#### 3.2 查询数据分布概况
`df.describe().show()`
```py
+-------+------------------+------+-----+------------------+
|summary|            userid|gender|level|               vip|
+-------+------------------+------+-----+------------------+
|  count|                 6|     6|    6|                 6|
|   mean|               3.5|  null| null|0.6666666666666666|
| stddev|1.8708286933869707|  null| null|0.5163977794943222|
|    min|              0001|     F|    H|                 0|
|    max|              0006|     M|    M|                 1|
+-------+------------------+------+-----+------------------+
```
## 4.行操作
#### 4.1 打印前3行
`df.show(3)`
```py
+------+------+-----+---+
|userid|gender|level|vip|
+------+------+-----+---+
|  0001|     F|    H|  1|
|  0002|     M|    M|  0|
|  0003|     F|    L|  1|
+------+------+-----+---+
only showing top 3 rows
```
#### 4.2 获取头3行到本地
`df.head(3)`或者`df.take(3)`，两者输出一样，注意数字不能为负。
```py
[Row(userid='0001', gender='F', level='H', vip=1),
 Row(userid='0002', gender='M', level='M', vip=0),
 Row(userid='0003', gender='F', level='L', vip=1)]
```
#### 4.3 行计数
`df.count()`
```py
6
```
#### 4.4 对null或nan的数据进行过滤：

```py
from pyspark.sql.functions import isnan,isnull
df1=df.filter(isnull('gender')) #把gender列里面数据为null的筛选出来（代表python的None类型）
df2=df.filter(isnan("vip"))  # 把vip列里面数据为nan的筛选出来（Not a Number，非数字数据）

+------+------+-----+---+
|userid|gender|level|vip|
+------+------+-----+---+
+------+------+-----+---+
```
#### 4.5 行去重
**输出是DataFrame:**
`df.select('level').distinct().show()`
```py
+-----+
|level|
+-----+
|    M|
|    L|
|    H|
+-----+
```
**输出是Row类:**
`df.select('level').distinct().collect()`
```py
[Row(level='M'), Row(level='L'), Row(level='H')]
```
**输出是List:**
`df.select('level').distinct().rdd.map(lambda r: r[0]).collect()`
```py
['M', 'L', 'H']
```
#### 4.6 随机抽样
`sample = df.sample(False,0.5,0)`
frac=0.5 随机抽取50%。
replace= True or False代表是否有放回。 
axis=0，对行进行抽样。
```py
+------+------+-----+---+
|userid|gender|level|vip|
+------+------+-----+---+
|  0002|     M|    M|  0|
|  0004|     F|    H|  0|
|  0005|     M|    M|  1|
+------+------+-----+---+
```
#### 4.7 按条件选择行
`df.where("gender='F' and level='H'").show()`
等价于`df.filter("gender='F' and level='H'").show()`
```py
+------+------+-----+---+
|userid|gender|level|vip|
+------+------+-----+---+
|  0001|     F|    H|  1|
|  0004|     F|    H|  0|
|  0006|     F|    H|  1|
+------+------+-----+---+
```

#### 4.8 删除有缺失值的行
`df2 = df.dropna()`
或者
`df2=df.na.drop()`

## 5.列操作
#### 5.1 获取DataFrame所有列名
`df.columns`
```py
['userid', 'gender', 'level', 'vip']
```
#### 5.2 选择一列或多列
`df.select('level').show()`
```py
+-----+
|level|
+-----+
|    H|
|    M|
|    L|
|    H|
|    M|
|    H|
+-----+
```
`df.select(df['gender'], df['vip']+1).show()`
```py
+------+---------+
|gender|(vip + 1)|
+------+---------+
|     F|        2|
|     M|        1|
|     F|        2|
|     F|        1|
|     M|        2|
|     F|        2|
+------+---------+
```
`df.select('userid','gender','level').show()`

等价于`df.select(df.userid, df.gender, df.level).show()`

等价于`df.select(df['userid'], df['gender'], df['level']).show()`
```py
+------+------+-----+
|userid|gender|level|
+------+------+-----+
|  0001|     F|    H|
|  0002|     M|    M|
|  0003|     F|    L|
|  0004|     F|    H|
|  0005|     M|    M|
|  0006|     F|    H|
+------+------+-----+
```
#### 5.3 按列排序
`df.orderBy(df.vip.desc()).show(5)`
等价于`df.sort('vip', ascending=False).show()`
```py
+------+------+-----+---+
|userid|gender|level|vip|
+------+------+-----+---+
|  0001|     F|    H|  1|
|  0005|     M|    M|  1|
|  0006|     F|    H|  1|
|  0003|     F|    L|  1|
|  0002|     M|    M|  0|
+------+------+-----+---+
only showing top 5 rows
```
#### 5.4 新增一列
**新增一列全部赋值为常量：**
```py
from pyspark.sql import functions
df = df.withColumn('label',functions.lit(0))
df.show()

+------+------+-----+---+-----+
|userid|gender|level|vip|label|
+------+------+-----+---+-----+
|  0001|     F|    H|  1|    0|
|  0002|     M|    M|  0|    0|
|  0003|     F|    L|  1|    0|
|  0004|     F|    H|  0|    0|
|  0005|     M|    M|  1|    0|
|  0006|     F|    H|  1|    0|
+------+------+-----+---+-----+
```
**通过另一个已有变量新增一列：**
`df2=df.withColumn('label',df.label+1)`
```py
+------+------+-----+---+-----+
|userid|gender|level|vip|label|
+------+------+-----+---+-----+
|  0001|     F|    H|  1|    1|
|  0002|     M|    M|  0|    1|
|  0003|     F|    L|  1|    1|
|  0004|     F|    H|  0|    1|
|  0005|     M|    M|  1|    1|
|  0006|     F|    H|  1|    1|
+------+------+-----+---+-----+
```
#### 5.5 修改列类型
```py
df = df.withColumn("label1", df["label"].cast("String"))
df.printSchema()
root
 |-- userid: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
 |-- vip: long (nullable = true)
 |-- label: integer (nullable = false)
 |-- label1: string (nullable = false)
```
#### 5.6 修改列名
`df3=df.withColumnRenamed( "label1" , "lbl" )`
——注意df自身没有发生变化。
```py
+------+------+-----+---+-----+---+
|userid|gender|level|vip|label|lbl|
+------+------+-----+---+-----+---+
|  0001|     F|    H|  1|    0|  0|
|  0002|     M|    M|  0|    0|  0|
|  0003|     F|    L|  1|    0|  0|
|  0004|     F|    H|  0|    0|  0|
|  0005|     M|    M|  1|    0|  0|
|  0006|     F|    H|  1|    0|  0|
+------+------+-----+---+-----+---+
```
#### 5.7 删除列
`df4=df.drop('length').show()`
```py
+------+------+-----+---+-----+
|userid|gender|level|vip|label|
+------+------+-----+---+-----+
|  0001|     F|    H|  1|    0|
|  0002|     M|    M|  0|    0|
|  0003|     F|    L|  1|    0|
|  0004|     F|    H|  0|    0|
|  0005|     M|    M|  1|    0|
|  0006|     F|    H|  1|    0|
+------+------+-----+---+-----+
```
### 6.选择和切片
#### 6.1 直接使用filter
`df.select('userid','gender','vip').filter(df['vip']>0).show()`
或者
`df.filter(df['vip']>0).select('userid','gender','vip').show()`,
前者select括号里面一定要有‘vip’这一列。

```py
+------+------+---+
|userid|gender|vip|
+------+------+---+
|  0001|     F|  1|
|  0003|     F|  1|
|  0005|     M|  1|
|  0006|     F|  1|
+------+------+---+
```
**多重选择**
`df.filter(df['vip']>0).filter(df['gender']=='F').show()`
```py
+------+------+-----+---+-----+------+
|userid|gender|level|vip|label|label1|
+------+------+-----+---+-----+------+
|  0001|     F|    H|  1|    0|     0|
|  0003|     F|    L|  1|    0|     0|
|  0006|     F|    H|  1|    0|     0|
+------+------+-----+---+-----+------+
```

#### 6.2 filter方法的SQL
`df.filter("gender='M'").show()`;
`df.filter("level like 'M%'").show()`
```py
+------+------+-----+---+-----+------+
|userid|gender|level|vip|label|label1|
+------+------+-----+---+-----+------+
|  0002|     M|    M|  0|    0|     0|
|  0005|     M|    M|  1|    0|     0|
+------+------+-----+---+-----+------+
```
#### 6.3 where方法的SQL
`df.where("level like 'M%'").show()`

#### 6.4 直接使用SQL语法
```py
# 首先dataframe注册为临时表，然后执行SQL查询
df.createOrReplaceTempView("df5")
spark.sql("select count(1) from df5").show()

+--------+
|count(1)|
+--------+
|       6|
+--------+
```
## 7. 合并 join / union
#### 7.1 横向拼接
`df.union(df).show()`或者`df.unionAll(df.limit(1)).show()`,二者好像没看出啥区别。

（未完待续。。）