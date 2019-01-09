package zxzl.recommend

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.math._

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
  * @param userid 用户
  * @param itemid 推荐物品
  * @param pref 评分
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

/**
  * 相似度计算.
  * 支持：同现相似度、欧氏距离相似度、余弦相似度
  *
  */
class ItemSimilarity extends Serializable {

  /**
    * 相似度计算.
    * @param user_rdd 用户评分
    * @param stype 计算相似度公式
    *  ItemSimi 返回物品相似度
    */
  def Similarity(user_rdd: Dataset[ItemPref], stype: String): (RDD[ItemSimi]) = {
    val simil_rdd = stype match {
      case "cooccurrence" =>
        ItemSimilarity.CooccurrenceSimilarity(user_rdd)
      case "cosine" =>
        ItemSimilarity.CosineSimilarity(user_rdd)
      case "euclidean" =>
        ItemSimilarity.EuclideanDistanceSimilarity(user_rdd)
      case "logLikelihoodSimilar" =>
        ItemSimilarity.logLikelihoodSimilar(user_rdd)
      case _ =>
        ItemSimilarity.CooccurrenceSimilarity(user_rdd)
    }
    simil_rdd
  }

}

object ItemSimilarity {

  /**
    * 同现相似度矩阵计算.
    * w(i,j) = N(i)∩N(j)/sqrt(N(i)*N(j))
    * @param user_rdd 用户评分
    *  ItemSimi 返回物品相似度
    *
    */
  /**
    * 同现相似度矩阵计算.
    * w(i,j) = N(i)∩N(j)/sqrt(N(i)*N(j))
    * @param user_rdd 用户评分
    *  ItemSimi 返回物品相似度
    *
    */
  def CooccurrenceSimilarity(user_rdd: Dataset[ItemPref]): (RDD[ItemSimi]) = {
    // val a = 0.5
    // 0 数据做准备
    val user_rdd1 = user_rdd.rdd.map(f => (f.userid, f.itemid, f.pref))  // (用户，物品，评分)
    val user_rdd2 = user_rdd1.map(f => (f._1, f._2))  // (用户，物品)
    // 1 (用户：物品) 笛卡尔积 (用户：物品) => 物品:物品组合
    val user_rdd3 = user_rdd2.join(user_rdd2)   // (用户,(物品1,物品2))   获取物品同现矩阵
    val user_rdd4 = user_rdd3.map(f => (f._2, 1)) // ((物品1,物品2),1)
    // 2 物品:物品:频次 ，按key求和
    val user_rdd5 = user_rdd4.reduceByKey((x, y) => x + y) // ((物品1,物品2),同现次数)) 单倍同现次数，正反是二倍同现次数
    // 3 对角矩阵
    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2) // 相同物品的同现记录  过滤出 一倍的同现次数
    // 4 非对角矩阵
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2) // 不同物品的同现记录  过滤出 单倍同现次数，正反二倍的同现次数
    // 5 计算同现相似度（物品1，物品2，同现频次）
    val user_rdd8 = user_rdd7.map(f => (f._1._1, (f._1._1, f._1._2, f._2)))
      .join(user_rdd6.map(f => (f._1._1, f._2))) // (物品1,((物品1,物品2,不同物品同现次数),物品1自同现次数))

    val user_rdd9 = user_rdd8.map(f => (f._2._1._2, (f._2._1._1,
      f._2._1._2, f._2._1._3, f._2._2)))  // (物品2,(物品1,物品2,不同物品同现,物品1自同现次数))
    val user_rdd10 = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2))) // （物品2,((物品1,物品2,不同物品同现,物品1自同现次数),物品2相同物品同现)


    val user_rdd11 = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2)) // (物品1,物品2,不同物品同现,物品1自同现次数，物品2相同物品同现)
    val user_rdd12 = user_rdd11.map(f => (f._1, f._2, (f._3 / sqrt(f._4 * f._5))))
    // 6 结果返回
    user_rdd12.map(f => ItemSimi(f._1, f._2, f._3))
  }

  /**
    * 余弦相似度矩阵计算.
    * T(x,y) = ∑x(i)y(i) / sqrt(∑(x(i)*x(i)) * ∑(y(i)*y(i)))
    * @param user_rdd 用户评分
    * ItemSimi 返回物品相似度
    */
  def CosineSimilarity(user_rdd: Dataset[ItemPref]): (RDD[ItemSimi]) = {
    // 0 数据做准备
    val user_rdd1 = user_rdd.rdd.map(f => (f.userid, f.itemid, f.pref))    // (用户,物品,评分)
    val user_rdd2 = user_rdd1.map(f => (f._1, (f._2, f._3)))           // (用户,(物品,评分))
    // 1 (用户,物品,评分) 笛卡尔积 (用户,物品,评分) => （物品1,物品2,评分1,评分2）组合       
    val user_rdd3 = user_rdd2.join(user_rdd2)                          // (用户,((物品1,评分),(物品2,评分)))
    val user_rdd4 = user_rdd3.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2))) // ((物品1,物品2),(评分1,评分2))
    // 2 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,评分1*评分2） 组合 并累加       
    val user_rdd5 = user_rdd4.map(f => (f._1, f._2._1 * f._2._2)).reduceByKey(_ + _)         // ( (物品1,物品2),∑(评分1 * 评分2) )
    // 3 对角矩阵 
    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2) //  ( (物品1,物品1), ∑(评分1 * 评分1) )
    // 4 非对角矩阵 
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2) //  ( (物品1,物品2), ∑(评分1 * 评分2) )
    // 5 计算相似度
    val user_rdd8 = user_rdd7.map(f => (f._1._1, (f._1._1, f._1._2, f._2)))
      .join(user_rdd6.map(f => (f._1._1, f._2))) // ( 物品1,(物品1,物品2,异购评分和）,物品1同购评分和)
    val user_rdd9 = user_rdd8.map(f => (f._2._1._2, (f._2._1._1,
      f._2._1._2, f._2._1._3, f._2._2)))   // ( 物品2 ,(物品1,物品2,异购评分和,物品1同购评分和) )
    val user_rdd10 = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))// ( 物品2 ,(物品1,物品2,异购评分和,物品1同购评分和)，物品2同购评分和）
    val user_rdd11 = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))  //  (物品1,物品2,异购评分和,物品1同购评分和 ，物品2同购评分和）
    val user_rdd12 = user_rdd11.map(f => (f._1, f._2, (f._3 / sqrt(f._4 * f._5)))) //
    // 6 结果返回
    user_rdd12.map(f => ItemSimi(f._1, f._2, f._3))
  }

  /**
    * 欧氏距离相似度矩阵计算.
    * d(x, y) = sqrt(∑((x(i)-y(i)) * (x(i)-y(i))))
    * sim(x, y) = n / (1 + d(x, y))
    * @param user_rdd 用户评分
    * ItemSimi 返回物品相似度
    *
    */
  def EuclideanDistanceSimilarity(user_rdd: Dataset[ItemPref]): (RDD[ItemSimi]) = {
    // 0 数据做准备
    val user_rdd1 = user_rdd.rdd.map(f => (f.userid, f.itemid, f.pref))    // (用户,物品,评分)
    val user_rdd2 = user_rdd1.map(f => (f._1, (f._2, f._3)))           // (用户,(物品,评分))
    // 1 (用户,物品,评分) 笛卡尔积 (用户,物品,评分) => （物品1,物品2,评分1,评分2）组合       
    val user_rdd3 = user_rdd2 join user_rdd2                           // ((物品1,评分),(物品2,评分))
    val user_rdd4 = user_rdd3.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2))) // ((物品1,物品2),(评分1,评分2) )
    // 2 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,评分1-评分2） 组合 并累加       
    val user_rdd5 = user_rdd4.map(f => (f._1, (f._2._1 - f._2._2) * (f._2._1 - f._2._2))).reduceByKey(_ + _) // ((物品1,物品2),(评分1-评分2) * (评分1-评分2))
    // 3 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,1） 组合 并累加    计算重叠数
    val user_rdd6 = user_rdd4.map(f => (f._1, 1)).reduceByKey(_ + _) // ( (物品1,物品2), 同现次数1)
    // 4 非对角矩阵 
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)   // ((物品1,物品2),∑(评分1-评分2) * (评分1-评分2))
    // 5 计算相似度
    val user_rdd8 = user_rdd7.join(user_rdd6)  // ((物品1,物品2) , (∑(评分1-评分2) * (评分1-评分2), 同现次数))
    val user_rdd9 = user_rdd8.map(f => (f._1._1, f._1._2, f._2._2 / (1 + sqrt(f._2._1))))
    // 6 结果返回
    user_rdd9.map(f => ItemSimi(f._1, f._2, f._3))
  }

  def logLikelihoodSimilar(user_rdd: Dataset[ItemPref]): (RDD[ItemSimi]) ={
    println("log比相似度")
    val data1 = user_rdd.rdd.map(x => (x.userid,(x.itemid,1))) // (userid,(itemid,1))
    val userCounts = data1.reduceByKey((x,y) => x).count()  // numUsers
    val itemCounts = user_rdd.rdd.map(x => (x.itemid,1)).reduceByKey(_+_) // itemId,userCount
    val data2 = data1.join(data1)      // (userid,((物品1,1),(物品2,1))
    val data3 = data2.map(x => ((x._2._1._1,x._2._2._1),1)).reduceByKey(_+_).filter(x => !x._1._1.equals(x._1._2)).map(x=>(x._1._1,(x._1._2,x._2))) //(物品1,物品2),counts
    data3.foreach( x => println("data3----> " + x))
    val data4 = data3.join(itemCounts) // (String,((String,Int),Int)) 物品1，物品2，同现数量，物品1数量
      .map(x =>(x._2._1._1,(x._1,x._2._1._2,x._2._2)))// 物品2，物品1，同现数量，物品1数量
      .join(itemCounts)// (物品2,((物品1，同现数量，物品1数量),物品2数量)
      .map{x =>  //println(x)
      ItemSimi(x._1,x._2._1._1,
        1.0 - 1.0/(1.0 + logLikelihoodRatioSimilarity(k11= x._2._1._2,  //同现
          k12= x._2._2 - x._2._1._2,//物品2 - 同现
          k21 = x._2._1._3 - x._2._1._2 ,//物品1 - 同现
          k22 = userCounts.toInt - x._2._1._3 - x._2._2 + x._2._1._2)))} // sum - 物品1 - 物品2 + 同现

    return data4
  }

  /**
    * 对于事件A和事件B，我们考虑两个事件发生的次数：
　　　* k11：事件A与事件B同时发生的次数
　　　* k12：B事件发生，A事件未发生
　　　* k21：A事件发生，B事件未发生
　　　* k22：事件A和事件B都未发生
    * rowEntropy = entropy(k11, k12) + entropy(k21, k22)
　　　* columnEntropy = entropy(k11, k21) + entropy(k12, k22)
　　　* matrixEntropy = entropy(k11, k12, k21, k22)
　　　* 对数似然相似度 2 * (matrixEntropy - rowEntropy - columnEntropy)
    */
  def logLikelihoodRatioSimilarity(k11:Int,k12:Int,k21:Int,k22:Int): Double={
    val rowEntropy = entropy(k11 + k12, k21 + k22)
    val columnEntropy = entropy(k11 + k21, k12 + k22)
    val matrixEntropy = entropy(k11, k12, k21, k22)
    if (rowEntropy + columnEntropy < matrixEntropy) {
      // round off error
      return 0.0
    }
    return 2.0 * (rowEntropy + columnEntropy - matrixEntropy)
  }

  def xLogX(x:Long):Double ={
    if(x == 0l) 0.0 else x * Math.log(x)
  }
  def entropy( a:Long,b:Long, c:Long,d:Long):Double = {
    xLogX(a + b + c + d) - xLogX(a) - xLogX(b) - xLogX(c) - xLogX(d)
  }
  def entropy(a: Long, b: Long) = xLogX(a + b) - xLogX(a) - xLogX(b)
  def entropy(elements:Long*) {
    var sum = 0l
    var result = 0.0
    for (  element <- elements) {
      result = result + xLogX(element)
      sum = sum + element
    }
    xLogX(sum) - result
  }
}