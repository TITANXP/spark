name := "spark"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.4"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.4"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.4"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.4.4"
// https://mvnrepository.com/artifact/org.scalanlp/breeze
libraryDependencies += "org.scalanlp" %% "breeze" % "1.0"
// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.11"
//引入kafka数据源的依赖
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.4.4"  exclude("net.jpountz.lz4", "lz4")


//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
//)