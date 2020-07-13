package test.spark.saveload

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

//Text文件的读取与保存
object TextFileTest {
  def main(args: Array[String]): Unit = {
    //读取
    val spark = new SparkContext("local[*]", "textfile")
    val rdd: RDD[String] = spark.textFile("in/a.txt")
      rdd.collect.foreach(println)
    //保存
    rdd.saveAsTextFile("in/aout")
    spark.stop()
  }
}
