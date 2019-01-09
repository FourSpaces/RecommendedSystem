package zxzl.recommend

import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

object Tool {

  def getHDFSFiles(hdfs_prefix: String, file_path: String): Array[String] ={
    val output = new Path(file_path)
    //  val hdfs = org.apache.hadoop.fs.FileSystem.get(
    //  new java.net.URI("hdfs://master:9000"), new org.apache.hadoop.conf.Configuration())
    val hdfs = FileSystem.get(new URI(hdfs_prefix), new org.apache.hadoop.conf.Configuration())

    // 获取列表数据
    val fs = hdfs.listStatus(output)
    val listPath = FileUtil.stat2Paths(fs)
    for(p <- listPath) yield p.toString()
  }

  def slice_(a: Array[String], s: Int): Array[String] = {

    var as = a
    if (a.length > abs(s)){
        as = if (s > 0) a.dropRight(a.length - s) else a.drop(a.length + s)
    }
    as

  }

  def abs(x:Int): Int = {
    if(x < 0) -x else x
  }
}


object TimeUtils {

  val now = new Date()

  def getTimestamp(): Long = {
    val a = now.getTime
    var str = a + ""
    str.substring(0, 10).toLong
  }

  def getDateTimestamp(): Long = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val a = dateFormat.parse(dateFormat.format(now)).getTime
    var str = a + ""
    str.substring(0, 10).toLong
  }

  def getDateString(simple: String="yyyy-MM-dd", distance:Int=0): String = {
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat(simple)
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,distance)
    var yesterday=dateFormat.format(cal.getTime())
    yesterday
  }

}
