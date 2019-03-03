---
layout: post
title:  "Pyspark技能记录：Dataframe操作"
categories: Pyspark
tags: 数据开发
author: MaShu
---

* content
{:toc}

数据分析师通常用Hive SQL进行操作，但其实可以考虑换成用Pyspark。Spark是基于内存计算的，比Hive快，PySpark SQL模块的很多函数和方法与Hive SQL中的关键字一样，Pyspark还可与Python中的模块结合使用
## 1. 连接本地spark
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```
## 2.创建DataFrame
```
sparkdf = spark.sql(""" select * from table_name """)
```