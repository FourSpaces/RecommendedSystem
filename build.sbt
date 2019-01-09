name := "RecommendedSystem"

version := "0.1"

scalaVersion := "2.11.7"

//添加依赖
libraryDependencies += "org.apache.spark" % "spark-parent_2.10" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.2.0"

//libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "2.2.0"

//libraryDependencies += "org.apache.spark" % "spark-mllib-local_2.10" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.2.0"

//libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.3"

//libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.10" % "2.2.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.9.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "2.2.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.9.1"

libraryDependencies += "org.eclipse.ecf" % "org.apache.log4j" % "1.2.15.v201012070815"

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "6.4.2"
