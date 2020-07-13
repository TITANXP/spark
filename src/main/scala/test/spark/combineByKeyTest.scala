package test.spark

import org.apache.spark.SparkContext

object combineByKeyTest {
  def main(args: Array[String]): Unit= {
    val spark = new SparkContext("local[*]", "combineByKey")
    val rdd = spark.parallelize(List(("a", 2), ("a", 1), ("b", 3), ("d", 2)))
      .combineByKey((_,1),(acc:(Int, Int), v) => (acc._1+v, acc._2+1), (acc:(Int, Int), acc2:(Int, Int)) => (acc._1+acc2._1, acc._2+acc2._2))
      .map{case(k,v) => (k,v._1 / v._2.toDouble)}
      .collect()
      .foreach(println)
    spark.stop();
  }

}
