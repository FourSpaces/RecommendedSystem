package zxzl.recommend

import org.apache.spark.sql.functions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.hive.HiveContext

object DataSource {

  def getUserItemPref_(hiveCtx: HiveContext, sparkSession: SparkSession): Dataset[ItemPref] = {
    /**
      *  获取用户评分数据
      **/

    import sparkSession.implicits._
    // 获取最近30天的日期
    val tab_count = hiveCtx.sql("select onlyid, sum(playtime_sum) as playtime_sum_total, sum(open_number) as open_number_total from userdb.play_game_time_cnt_like_rate where ymd >= '20181110' and playtime_sum > 60 and playtime_sum < 3600 group by onlyid")

    val tab_each_count = hiveCtx.sql("select onlyid as imei_b, packagename, sum(playtime_sum) as playtime_sum, sum(open_number) as open_number from userdb.play_game_time_cnt_like_rate where ymd >= '20181110' and playtime_sum > 60 and playtime_sum < 3600 group by onlyid, packagename")

    // 计算得分
    val join_pack = tab_each_count.join(tab_count, tab_each_count("imei_b") === tab_count("onlyid"))
    val join_pack_1 = join_pack.withColumn("playtime_ratio", functions.bround($"playtime_sum"/$"playtime_sum_total", 4))
    val join_pack_2 = join_pack_1.withColumn("open_ration", functions.bround($"open_number"/$"open_number_total", 4))
    val join_pack_3 = join_pack_2.withColumn("ugld", functions.bround($"playtime_ratio" * $"open_ration", 4)).drop("imei_b")

    // 保存数据
    join_pack_3.write.mode("overwrite").format("parquet").saveAsTable("userdb.user_game_level")
    // 返回指定格式的数据
    join_pack_3.select($"onlyid".as("userid"), $"packagename".as("itemid"), $"ugld".as("pref")).as[ItemPref]
  }

  def getUserItemPref(hiveCtx: HiveContext, startDate: String, sparkSession: SparkSession): Dataset[ItemPref] = {
    /**
      *  获取用户评分数据
      **/

    import sparkSession.implicits._
    // 获取最近30天的日期
    val tab_count = hiveCtx.sql("select onlyid, sum(playtime_sum) as playtime_sum_total, sum(open_number) as open_number_total from userdb.play_game_time_cnt_like_rate where ymd >= '%s' and playtime_sum > 60 and playtime_sum < 3600 group by onlyid".format(startDate))

    val tab_each_count = hiveCtx.sql("select onlyid as imei_b, packagename, sum(playtime_sum) as playtime_sum, sum(open_number) as open_number from userdb.play_game_time_cnt_like_rate where ymd >= '%s' and playtime_sum > 60 and playtime_sum < 3600 group by onlyid, packagename".format(startDate))

    // 计算得分
    val join_pack = tab_each_count.join(tab_count, tab_each_count("imei_b") === tab_count("onlyid"))
    val join_pack_1 = join_pack.withColumn("playtime_ratio", functions.bround($"playtime_sum"/$"playtime_sum_total", 4))
    val join_pack_2 = join_pack_1.withColumn("open_ration", functions.bround($"open_number"/$"open_number_total", 4))
    // val join_pack_3 = join_pack_2.withColumn("ugld", functions.bround($"playtime_ratio" * $"open_ration", 4)).drop("imei_b")
    val join_pack_3 = join_pack_2.withColumn("ugld", $"open_ration" * $"open_ration").drop("imei_b")

    // 保存数据
    join_pack_3.write.mode("overwrite").format("parquet").saveAsTable("userdb.user_game_level")
    // 返回指定格式的数据
    join_pack_3.select($"onlyid".as("userid"), $"packagename".as("itemid"), $"ugld".as("pref")).as[ItemPref]
  }
}