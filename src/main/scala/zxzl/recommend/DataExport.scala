package zxzl.recommend

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

object DataExport {

//  def SaveHive(spark: SparkSession, recommd_rdd1: (RDD[UserRecomm])): Unit = {
//    // 将结果保存到 hive 中
//
//    // 判断表，创建表
//    spark.sql("use recommend")
//
//    val hive_create_table = "CREATE TABLE IF NOT EXISTS user_recomm ( userid String, itemid String, pref Float)"+" COMMENT 'User recommendation result'"+" ROW FORMAT DELIMITED"+" FIELDS TERMINATED BY '\t'"+" LINES TERMINATED BY '\n'"+" STORED AS TEXTFILE"
//    spark.sql(hive_create_table)
//
//    // 保存数据到 Hive 中
//    recommd_rdd1.toDF().registerTempTable("tempTable")
//    spark.sql("insert into recommend.user_recomm select * from tempTable")
//  }

  def SaveES(recommd_rdd: (RDD[UserRecomm])): Unit = {
    // 将结果保存到 ES 中

    val storage_path = "recommended/users_recomm"

    EsSpark.saveToEs(recommd_rdd, storage_path, Map("es.mapping.id" -> "recommid"))

    // 删除 ES中 超过 30 天的记录， [ 后续补充 上删除结果 ]

    val interval = 3600 * 24 * 30

    val timestamp = TimeUtils.getDateTimestamp()

  }
}
