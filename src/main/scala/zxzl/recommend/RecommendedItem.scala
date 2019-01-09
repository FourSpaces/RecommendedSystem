package zxzl.recommend

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, functions}

/**
  * 用户推荐计算.
  * 根据物品相似度、用户评分、指定最大推荐数量进行用户推荐
  */

class RecommendedItem {
  /**
    * 用户推荐计算.
    * @param items_similar 物品相似度
    * @param user_prefer 用户评分
    * @param r_number 推荐数量
    * @param RDD[UserRecomm] 返回用户推荐物品
    *
    */

  def Recommend(items_similar: RDD[ItemSimi],
                user_prefer: Dataset[ItemPref],
                r_number: Int): (RDD[UserRecomm]) = {
    //   0 数据准备
    val rdd_app1_R1 = items_similar.map(f => (f.itemid1, f.itemid2, f.similar))
    val user_prefer1 = user_prefer.rdd.map(f => (f.userid, f.itemid, f.pref))

    //   1 矩阵计算——i行与j列join， ( f._1, (f._2, f._3), (f._1, f._3) )
    //   (com.xiaobanlong.main,((com.huluxia.gametools,0.4472135954999579),(A000006AC02B0D,1.0)))
    val rdd_app1_R2 = rdd_app1_R1.map(f => (f._1, (f._2, f._3))).
      join(user_prefer1.map(f => (f._2, (f._1, f._3))))

    //   2 矩阵计算——i行与j列元素相乘
    //   ((A000006AC02B0D,com.huluxia.gametools),0.4472135954999579)
    val rdd_app1_R3 = rdd_app1_R2.map(f => ((f._2._2._1, f._2._1._1), f._2._2._2 * f._2._1._2))

    //   3 矩阵计算——用户：元素累加求和
    val rdd_app1_R4 = rdd_app1_R3.reduceByKey((x, y) => x + y)

    //   4 矩阵计算——用户：对结果过滤已有I2
    //   (863454037607772,(com.kamizoto.thereisnogame,0.1873171623163388))
    val rdd_app1_R5 = rdd_app1_R4.leftOuterJoin(user_prefer1.map(f => ((f._1, f._2), 1))).
      filter(f => f._2._2.isEmpty).map(f => (f._1._1, (f._1._2, f._2._1)))

    //   5 矩阵计算——用户：用户对结果排序，过滤
    val rdd_app1_R6 = rdd_app1_R5.groupByKey()
    val rdd_app1_R7 = rdd_app1_R6.map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > r_number) i2_2.remove(0, (i2_2.length - r_number))
      (f._1, i2_2.toIterable)
    })
    val rdd_app1_R8 = rdd_app1_R7.flatMap(f => {
      val id2 = f._2
      for (w <- id2) yield (f._1, w._1, w._2)
    })

    val timestamp = TimeUtils.getDateTimestamp()
    rdd_app1_R8.map(f => UserRecomm(f._1+"_"+f._2, f._1, f._2, f._3, timestamp))
  }

  /**
    *
    * @param items_similar0
    * @param user_prefer
    * @param r_number
    * @return
    */
  def recommend(items_similar0: Dataset[ItemSimi], user_prefer: Dataset[ItemPref],
                r_number: Int, sparkSession: SparkSession): Dataset[UserRecomm] = {

    import sparkSession.implicits._
    // 0 数据准备, 选取最相似的 r_number 个游戏
    val simil_rdd0 = items_similar0.sort($"itemid1", $"similar".desc)
    val simil_rdd1 = simil_rdd0.rdd.map(line=>(line.itemid1, line)).groupByKey()
    val items_similar = simil_rdd1.map(m=>{m._2.take(r_number)}).flatMap(lien => lien).toDS

    //   1 矩阵计算——i行与j列join， ( f._1, (f._2, f._3), (f._1, f._3) )
    //   (com.xiaobanlong.main,((com.huluxia.gametools, 0.4472135954999579),(A000006AC02B0D,1.0)))
    val dataset_app1_R2 = user_prefer.join(items_similar, items_similar.col("itemid1")===user_prefer.col("itemid"))

    //   2 矩阵计算——i行与j列元素相乘
    //   ((A000006AC02B0D,com.huluxia.gametools),0.4472135954999579)
    val dataset_app1_R3 = dataset_app1_R2.withColumn("prediction_pref", functions.bround($"pref"*$"similar", 4))

    //   3 矩阵计算——用户：元素累加求和. 整理
    val dataset_app1_R4 = dataset_app1_R3.groupBy($"userid", $"itemid2").sum("prediction_pref")
    val dataset_app1_R5 = dataset_app1_R4.drop("itemid1").withColumnRenamed("sum(prediction_pref)", "pref")
    val dataset_app1_R6 = dataset_app1_R5.withColumn("timestamp", functions.current_timestamp())

    val dataset_app1_R7 = dataset_app1_R6.withColumnRenamed("itemid2", "itemid").withColumn("pref", functions.bround($"pref", 4))
    val dataset_app1_R8 = dataset_app1_R7.withColumn("recommid", functions.concat($"userid", $"itemid")).as[UserRecomm]
    //.as[UserRecomm]

    // 排列相关度，获取最高的
    val dataset_app1_R9 = dataset_app1_R8.sort($"userid", $"pref".desc)
    val dataset_app1_R10 = dataset_app1_R9.rdd.map(line =>(line.userid, line)).groupByKey()
    val dataset_app1_R11 = dataset_app1_R10.map(line=>{line._2.take((line._2.size * 0.5).toInt)}).flatMap(lien =>lien).toDS
    dataset_app1_R11
  }

}