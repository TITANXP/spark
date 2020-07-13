package test.spark.saveload

import org.apache.spark.SparkContext
//Object文件的读取和保存
object ObjectFileTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "obj")
    //创建一个RDD
    val rdd = spark.makeRDD(List(1, 2, 3))
    //将RDD保存为Onject文件
    rdd.saveAsObjectFile("out/object1")

    //读取Object文件
    val objFile = spark.objectFile[Int]("out/object1")
      .collect().foreach(println)
    spark.stop()
  }
}
