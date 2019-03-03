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
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```
## 2.创建DataFrame
**用Spark SQL创建DataFrame:**
```
sparkdf = spark.sql(""" select * from table_name """)
```
**手动创建DataFrame**
```
df = spark.createDataFrame([('0001','F','H',1), ('0002','M','M',0), ('0003','F','L',1),
                            ('0004','F','H',0), ('0005','M','M',1), ('0006','F','H',1)
                           ], ['userid','gender','level','vip'])
```
## 3.查询
#### 3.1 概况查询
##### 3.1.1 以树形式打印概要
`df.printSchema()`
```
root
 |-- userid: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
 |-- vip: long (nullable = true)
```
##### 3.1.2 查询概况
`df.describe().show()`
```
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
#### 3.2 行查询
##### 3.2.1 打印前3行
`df.show(3)`
```
+------+------+-----+---+
|userid|gender|level|vip|
+------+------+-----+---+
|  0001|     F|    H|  1|
|  0002|     M|    M|  0|
|  0003|     F|    L|  1|
+------+------+-----+---+
only showing top 3 rows
```
##### 3.2.2 获取头3行到本地
`df.head(3)`或者`df.take(3)`，两者输出一样，注意数字不能为负。
```
[Row(userid='0001', gender='F', level='H', vip=1),
 Row(userid='0002', gender='M', level='M', vip=0),
 Row(userid='0003', gender='F', level='L', vip=1)]
```
##### 3.2.3 行计数
`df.count()`
```
6
```
##### 3.2.4 查询某列为取值为空的行
```
from pyspark.sql.functions import isnull
df1=df.filter(isnull('gender'))
df1.show()

+------+------+-----+---+
|userid|gender|level|vip|
+------+------+-----+---+
+------+------+-----+---+
```
##### 3.3.5 行去重
**输出是DataFrame:**
`df.select('level').distinct().show()`
```
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
```
[Row(level='M'), Row(level='L'), Row(level='H')]
```
**输出是List:**
`df.select('level').distinct().rdd.map(lambda r: r[0]).collect()`
```
['M', 'L', 'H']
```


