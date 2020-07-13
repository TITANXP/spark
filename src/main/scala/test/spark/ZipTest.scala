package test.spark

import org.apache.spark.SparkContext

object ZipTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "zip")
    val rdd1 = spark.parallelize(List(1,2), 2)
    val rdd2 = spark.parallelize(List("a", "b"),2)
    rdd1.zip(rdd2)
      .collect
      .foreach(println)
    spark.stop()
  }
}
