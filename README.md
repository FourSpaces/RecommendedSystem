# RecommendedSystem
推荐系统引擎部分 spark 实现 ItemCF 算法

ItemCF.scala 是本系统的入口文件。

## 系统流程
- 读取数据
- 计算 Item 的相似度
- 根据计算好的 Item 相似度，进行推荐计算
- 保存推荐结果，这里保存到了ES中
- 计算统计 准确度和召回率

## 代码构成：
- ItemCF.scala : 实现了推荐系统的主要流程，见上所述 系统流程
- Tool.scala : 实现了系统中使用的一些小工具，包括时间函数、字符串函数
- DataSource.scala : 实现了数据载入，大家需要改写此文件，使得它符合自己的项目
- ItemSimilarity.scala : 实现了 相似度计算 算法
- RecommendedItem.scala : 实现了 推荐计算 算法
- DataExport.scala : 实现了 数据导出的保存，目前保存到ES 中，请根据自身需求改写
- Evaluation.scala : 实现了精准度与召回率的计算

## 数据格式：

代码中使用的数据格式如下所示，实现在 ItemSimilarity.scala 中
``` scala
/**
  * 用户评分.
  * @param userid 用户
  * @param itemid 评分物品
  * @param pref 评分
  */
case class ItemPref(
                     val userid: String,
                     val itemid: String,
                     val pref: Double) extends Serializable
/**
  * 用户推荐.
  * @param recommid 推荐唯一标识
  * @param userid 用户
  * @param itemid 推荐物品
  * @param pref 评分
  * @param timestamp 时间
  */
case class UserRecomm( val recommid: String,
                       val userid: String,
                       val itemid: String,
                       val pref: Double,
                       val timestamp: Long
                     ) extends Serializable
/**
  * 相似度.
  * @param itemid1 物品
  * @param itemid2 物品
  * @param similar 相似度
  */
case class ItemSimi(
                     val itemid1: String,
                     val itemid2: String,
                     val similar: Double) extends Serializable
```



### 注意：
此项目仅仅实现了推荐系统的 推荐引擎模块，业务模块和存储模块，并不在此项目中。

业务模块和存储模块 请参考 我的另一个项目： [影视推荐系统网站部分](https://github.com/FourSpaces/RecommendationMovie)

这两个项目并非 完美契合，需要大家根据自己的需求改动，才能构成一个完美的推荐系统

主要修改的部分为：

    推荐引擎项目中的 数据导入部分，数据导出部分。
    
    将数据格式修改为推荐引擎所需要的格式，即可运行，实现推荐
