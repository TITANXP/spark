package test.sparksql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_11_mysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("11")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //从MySql读数据
    //方式1
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/pms?useSSL=false&serverTimezone=UTC")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "position")
      .option("user", "root")
      .option("password", "root")
      .load()
    df.show()
    //方式2
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")
    properties.put("driver", "com.mysql.cj.jdbc.Driver")
    val df2 = spark.read.jdbc("jdbc:mysql://localhost:3306/pms?useSSL=false&serverTimezone=UTC", "position", properties)
    df2.show()
    //方式3
    val map: Map[String , String] = Map[String, String](
      "url" -> "jdbc:mysql://localhost:3306/pms?useSSL=false&serverTimezone=UTC",
      "driver" -> "com.mysql.cj.jdbc.Driver",
      "dbtable" -> "position",
      "user" -> "root",
      "password" -> "root"
    )
    val df3 = spark.read.format("jdbc").options(map).load()
    df3.show()

    //向MySql写数据
    //方式1
    df.write
        .mode("append")
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/pms?useSSL=false&serverTimezone=UTC")
        .option("dbtable", "position1") //如果表不存在则会创建
        .option("user", "root")
        .option("password", "root")
        .save()
    //方式2
    val properties2 = new Properties()
    properties2.put("user", "root")
    properties2.put("password", "root")
    properties2.put("driver", "com.mysql.cj.jdbc.Driver")
    df.write.mode("append").jdbc("jdbc:mysql://localhost:3306/pms?useSSL=false&serverTimezone=UTC", "position1", properties2)
    //方式3
    val map2: Map[String , String] = Map[String, String](
      "url" -> "jdbc:mysql://localhost:3306/pms?useSSL=false&serverTimezone=UTC",
      "driver" -> "com.mysql.cj.jdbc.Driver",
      "dbtable" -> "position",
      "user" -> "root",
      "password" -> "root"
    )
    df.write.format("jdbc").mode("append").options(map2)
    spark.stop()
  }
}
