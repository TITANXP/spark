package test.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object joinAndCogroupTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "join")
    val rdd1 = spark.makeRDD(List(("a", 1), ("b", 1), ("c", 1)))
    val rdd2 = spark.makeRDD(List(("a", 2), ("b", 2), ("d", 2)))
    rdd1.join(rdd2).join(rdd2)
      .collect()
      .foreach(println)
    rdd1.cogroup(rdd2)
        .collect()
        .foreach(println)
    spark.stop()
  }
}
