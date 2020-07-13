package test.spark

import org.apache.spark.{SparkConf, SparkContext}

object mapValueTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("a")
    val spark = SparkContext.getOrCreate(conf)
    val rdd = spark.makeRDD(List(("a", 1), ("b", 2)))
      .mapValues(_+2)
      .collect()
      .foreach(println)
    spark.stop()
  }

}
