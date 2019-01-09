package zxzl.recommend

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

// 计算准确度与召回率
case class People(userid:String, user_item_group1:List[String],  user_item_len:Int, recommd_group1:List[String], recommd_len:Int, Intersection:Int)


class Evaluations(recommd_rdd: org.apache.spark.sql.Dataset[UserRecomm],
                  user_item:org.apache.spark.sql.Dataset[ItemPref], sparkSession: SparkSession) extends Serializable {
  import sparkSession.implicits._
  import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, functions}
  var hit = 0
  var all = 0

  val recommd_group1 = recommd_rdd.rdd.map(line => (line.userid, line.itemid)).groupByKey()
  val user_item_group1 = user_item.rdd.map(line => (line.userid, line.itemid)).groupByKey()

  val statistics_group1 = user_item_group1.join(recommd_group1)

  val statistics_group2 = statistics_group1.map(line => People(line._1, line._2._1.toList, line._2._1.size, line._2._2.toList,  line._2._2.size, line._2._1.toList.intersect(line._2._2.toList).length)).toDS

  // 计算准确率
  val statistics_group3 = statistics_group2.agg("user_item_len"->"sum", "recommd_len"->"sum", "Intersection"->"sum")
  val statistics_group4 = statistics_group3.withColumn("recall", functions.bround($"sum(Intersection)"/$"sum(user_item_len)", 4))
  val statistics_group5 = statistics_group4.withColumn("precision", functions.bround($"sum(Intersection)"/$"sum(recommd_len)", 4))


  def recallRate(): org.apache.spark.sql.DataFrame = {
    statistics_group5
  }

  def precisionRate(): org.apache.spark.sql.DataFrame = {
    statistics_group5
  }

  def statistics(): org.apache.spark.sql.DataFrame = {
    statistics_group5
  }
}
