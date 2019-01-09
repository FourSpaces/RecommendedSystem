package zxzl.recommend

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object ItemCF {

  def main(args: Array[String]): Unit ={

    val logger:Logger=LoggerFactory.getLogger("ItemCF")

    println(args.toString)
    println("=============<< spark start run >>==============")

    // 构建 spark 对象，配置 ES环境变量
    val conf = new SparkConf().setAppName("ItemCF").setMaster("yarn")
                      .set("spark.es.index.auto.create", "true")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.config(conf).getOrCreate()

    val hiveCtx = new HiveContext(sc)

    //载入数据
    import spark.implicits._

    // 调高日志级别
    sc.setLogLevel("warn")

    val startDate = TimeUtils.getDateString(distance=0-60)
    val startDate_ = TimeUtils.getDateString(distance=0-10)

    // 读取配置文件，动态配置
    logger.warn(s"开始计算推荐：\n 1 获取用户每日统计数据：")

    // 读取数据
    val user_item_pref = DataSource.getUserItemPref(hiveCtx, startDate, spark)

    val user_item_pref_ = DataSource.getUserItemPref(hiveCtx, startDate_, spark)

    // 建立模型，进行计算，保存游戏相似度表
    logger.warn(s"2 计算游戏相似度：\n")
    val mysimil = new ItemSimilarity()
    val simil_rdd1 = mysimil.Similarity(user_item_pref, "cooccurrence").toDS() //logLikelihoodSimilar   cooccurrence

    // 输出相似度计算结果，保存推荐表
    logger.warn(s"3 计算推荐列表：\n")
    val recommd = new RecommendedItem()
    val recommd_rdd1 = recommd.recommend(simil_rdd1, user_item_pref_, 8, spark)

    logger.warn(s"4 将推荐结果导入 ES, hive \n")
    try{
      DataExport.SaveES(recommd_rdd1.rdd)
    }catch{
      case ex: Throwable =>println("推荐结果导入ES 失败，请检查: "+ ex)
    }

    // 计算准确度和召回率
    logger.warn(s"5 计算准确率与召回率, hive \n")
    val evaluations = new Evaluations(recommd_rdd1, user_item_pref_,spark)
    evaluations.statistics.show()

    spark.stop()
    logger.warn(s"计算完成\n")
  }

}