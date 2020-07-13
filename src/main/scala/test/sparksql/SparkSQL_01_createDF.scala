package test.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_01_createDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparksqk01")
    val spark = SparkSession.builder().config(conf).getOrCreate();

    //读取JSON文件
    val dataFrame = spark.read.json("in/people.json")
    dataFrame.show()
    spark.stop()
  }
}
