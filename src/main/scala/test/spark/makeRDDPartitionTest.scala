package test.spark

import org.apache.spark.SparkContext
//测试makeRDD的分区算法
object makeRDDPartitionTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "makerdd")
    val rdd = spark.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)
      .glom()
      .collect()
      .foreach(a => println(a.size))
    spark.stop()
  }
}
