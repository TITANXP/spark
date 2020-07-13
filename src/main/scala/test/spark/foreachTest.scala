package test.spark

import org.apache.spark.SparkContext

object foreachTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "foreach")
    val rdd = spark.makeRDD(List(1, 2, 3))
      rdd.foreach(println)
    spark.stop()
  }
}
