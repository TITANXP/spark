package test.spark.cacheAndCheckpoint

import org.apache.spark.SparkContext

//缓存计算结果
object PersistTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "persist")
    val rdd = spark.makeRDD(List(1))
      .map(""+System.currentTimeMillis())
      .cache()

    //缓存后打印出的时间相同
    rdd.collect().foreach(println)
    rdd.collect().foreach(println)
    spark.stop()
  }
}
