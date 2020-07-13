package test.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//读取JSON
object SparkSQL_09_JSON {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparksql09")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val df = spark.read.json("in/people.json")
    val df2 = spark.read.format("json").load("in/people.json")
    spark.stop()
  }
}
