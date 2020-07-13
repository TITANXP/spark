package test.spark

import org.apache.spark.SparkContext

object aggregateByKeyTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "agg")
    val rdd = spark.parallelize(List(("a", 2), ("a", 1), ("b", 3), ("c", 5)), 2)
      .aggregateByKey(0)(math.max(_,_), _+_)
      .collect()
      .foreach(println)

    spark.stop()
  }
}
