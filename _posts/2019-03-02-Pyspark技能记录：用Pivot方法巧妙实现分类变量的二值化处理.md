---
layout: post
title:  "Pyspark技能记录：用Pivot方法巧妙实现分类变量的二值化处理"
categories: Pyspark
tags: 数据开发
author: MaShu
---

* content
{:toc}

机器学习中特征变量有两种：分类变量和顺序变量，对分类变量需要进行二值化处理，Pyspark也提供One-Hot方法，但和sklearn中的One-Hot一样，新的变量不能保留原变量名称，但是我们可以用Pivot方法来巧妙实现分类变量的二值化处理。

## 二值化处理的效果如下：
数据处理前：
```
+------+------+-----+---+
|userid|gender|level|vip|
+------+------+-----+---+
|  0001|     F|    H|  1|
|  0002|     M|    M|  0|
|  0003|     F|    L|  1|
|  0004|     F|    H|  0|
|  0005|     M|    M|  1|
|  0006|     F|    H|  1|
+------+------+-----+---+ 
```
数据处理后：
```
+------+------+-----+---+---+--------+--------+-------+-------+-------+-----+-----+
|userid|gender|level|vip|  x|gender_F|gender_M|level_L|level_M|level_H|vip_0|vip_1|
+------+------+-----+---+---+--------+--------+-------+-------+-------+-----+-----+
|  0002|     M|    M|  0|  1|     0.0|     1.0|    0.0|    1.0|    0.0|  1.0|  0.0|
|  0003|     F|    L|  1|  1|     1.0|     0.0|    1.0|    0.0|    0.0|  0.0|  1.0|
|  0001|     F|    H|  1|  1|     1.0|     0.0|    0.0|    0.0|    1.0|  0.0|  1.0|
|  0006|     F|    H|  1|  1|     1.0|     0.0|    0.0|    0.0|    1.0|  0.0|  1.0|
|  0004|     F|    H|  0|  1|     1.0|     0.0|    0.0|    0.0|    1.0|  1.0|  0.0|
|  0005|     M|    M|  1|  1|     0.0|     1.0|    0.0|    1.0|    0.0|  0.0|  1.0|
+------+------+-----+---+---+--------+--------+-------+-------+-------+-----+-----+
```
## 如何实现二值化？——Pivot方法介绍
**什么是Pivot?**
具体见下图：
![](https://github.com/tjmashu/tjmashu.github.io/blob/master/pics/Pivot%20%E5%92%8CUnpivot.png?raw=true)
先看下面的例子了解下PySpark DataFrame的Pivot实现方法：
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import pyspark.sql.functions as F

# 原始数据 
df = spark.createDataFrame([('0001','F','H',1), ('0002','M','M',0), ('0003','F','L',1),
                            ('0004','F','H',0), ('0005','M','M',1), ('0006','F','H',1)
                           ], ['userid','gender','level','vip'])

```
样本数据如下:
```
+------+------+-----+---+
|userid|gender|level|vip|
+------+------+-----+---+
|  0001|     F|    H|  1|
|  0002|     M|    M|  0|
|  0003|     F|    L|  1|
|  0004|     F|    H|  0|
|  0005|     M|    M|  1|
|  0006|     F|    H|  1|
+------+------+-----+---+                        
```
透视操作简单直接，逻辑如下(这种方法类似SQL中的case...when...then...else...end):

- 按照不需要转换的字段分组，本例中是level；
- 使用pivot函数进行透视，透视过程中可以提供第二个参数来明确指定使用哪些数据项；
- 汇总数字字段，本例中是userid；

代码如下:
```
df_pivot = df.groupBy('gender')\
                .pivot('level', ['H','M','L'])\
                .agg(F.countDistinct('userid'))\
                .fillna(0)
```
结果如下:
```
+------+---+---+---+
|gender|  H|  M|  L|
+------+---+---+---+
|     F|  3|  0|  1|
|     M|  0|  2|  0|
+------+---+---+---+  
```


## 如何实现二值化？——Pivot方法技巧应用

受这种方法启发，我们可以对原数据新增一列：
```
+------+------+-----+---+---+
|userid|gender|level|vip|  x|
+------+------+-----+---+---+
|  0001|     F|    H|  1|  1|
|  0002|     M|    M|  0|  1|
|  0003|     F|    L|  1|  1|
|  0004|     F|    H|  0|  1|
|  0005|     M|    M|  1|  1|
|  0006|     F|    H|  1|  1|
+------+------+-----+---+---+
```
然后对每一个分类特征变量列,以主键（主键意味着不重复，这里是user_id）为分组，进行聚合（sum和avg都可以）：

```
newdf_pivot = newdf.groupBy('userid')\
                        .pivot(col, value_sets)\
                        .agg(F.avg('x'))\
                        .fillna(0)
```
于是得到若干个列切片的Dataframe, 这里user_id重命名成uid是为了跟主键user_id区别:
```
+----+--------+--------+
| uid|gender_F|gender_M|
+----+--------+--------+
|0002|     0.0|     1.0|
|0003|     1.0|     0.0|
|0001|     1.0|     0.0|
|0006|     1.0|     0.0|
|0004|     1.0|     0.0|
|0005|     0.0|     1.0|
+----+--------+--------+

+----+-------+-------+-------+
| uid|level_L|level_M|level_H|
+----+-------+-------+-------+
|0002|    0.0|    1.0|    0.0|
|0003|    1.0|    0.0|    0.0|
|0001|    0.0|    0.0|    1.0|
|0006|    0.0|    0.0|    1.0|
|0004|    0.0|    0.0|    1.0|
|0005|    0.0|    1.0|    0.0|
+----+-------+-------+-------+

+----+-----+-----+
| uid|vip_0|vip_1|
+----+-----+-----+
|0002|  1.0|  0.0|
|0003|  0.0|  1.0|
|0001|  0.0|  1.0|
|0006|  0.0|  1.0|
|0004|  1.0|  0.0|
|0005|  0.0|  1.0|
+----+-----+-----+
```
然后把原Dataframe跟上面的Dataframe进行列合并，去掉uid列，最终得到：
```
+------+------+-----+---+---+--------+--------+-------+-------+-------+-----+-----+
|userid|gender|level|vip|  x|gender_F|gender_M|level_L|level_M|level_H|vip_0|vip_1|
+------+------+-----+---+---+--------+--------+-------+-------+-------+-----+-----+
|  0002|     M|    M|  0|  1|     0.0|     1.0|    0.0|    1.0|    0.0|  1.0|  0.0|
|  0003|     F|    L|  1|  1|     1.0|     0.0|    1.0|    0.0|    0.0|  0.0|  1.0|
|  0001|     F|    H|  1|  1|     1.0|     0.0|    0.0|    0.0|    1.0|  0.0|  1.0|
|  0006|     F|    H|  1|  1|     1.0|     0.0|    0.0|    0.0|    1.0|  0.0|  1.0|
|  0004|     F|    H|  0|  1|     1.0|     0.0|    0.0|    0.0|    1.0|  1.0|  0.0|
|  0005|     M|    M|  1|  1|     0.0|     1.0|    0.0|    1.0|    0.0|  0.0|  1.0|
+------+------+-----+---+---+--------+--------+-------+-------+-------+-----+-----+
```

全部代码：
```
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pyspark.sql.functions as F

if __name__ == '__main__':

    spark = SparkSession.builder.getOrCreate()
    # 创建DataFrame
    sparkdf = = spark.createDataFrame([('0001','F','H',1), ('0002','M','M',0), ('0003','F','L',1),
                            ('0004','F','H',0), ('0005','M','M',1), ('0006','F','H',1)
                           ], ['userid','gender','level','vip']) 
    # 新增一列，列取值全部为1
    newdf = sparkdf.withColumn("x", lit(1)) 
    # 获取列名称
    colnames=sparkdf.columns 
	# 遍历列
    for col in colnames: 
     	# 非分类变量列跳过操作
        if col<>'userid':
            # 获取每个分类变量的取值范围，这一点就不用像SQL中的case...when...then...else...end那样需要手动穷举啦
            value_sets=sparkdf.select(col).distinct().rdd.map(lambda r: r[0]).collect() 
            newdf_pivot = newdf.groupBy('userid')\ # 使用主键分组
                        .pivot(col, value_sets)\ # 按照分类变量取值进行聚合
                        .agg(F.avg('x'))\
                        .fillna(0) # 分组中没有该取值的则为0
            # 为了保留列变量名名称，重命名新的列为：列名_取值
            for value in value_sets: 
                newdf_pivot=newdf_pivot.withColumnRenamed(str(value),col+'_'+str(value))
            newdf_pivot=newdf_pivot.withColumnRenamed('userid','uid') # 为了与原Dataframe的主键userid区分,重命名userid为uid

            newdf_pivot.show()
            # 将进行过二值化处理的新Dataframe左连接合并到原Dataframe
            newdf=newdf.join(newdf_pivot, newdf.userid == newdf_pivot.uid, "inner") 
            # 把uid列去掉
            newdf=newdf.drop('uid') 
    newdf.show()

```

**想想，如果有空值要怎么操作**
我的答案是——遍历列时，先把空值赋值为'NA'(String),-1(Int),只要其他取值能不一样就好，其他代码一样。
