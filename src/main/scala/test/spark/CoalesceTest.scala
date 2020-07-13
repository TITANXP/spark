package test.spark

import org.apache.spark.{SparkConf, SparkContext}

object CoalesceTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("coalesce")
    val spark = SparkContext.getOrCreate(conf)
    val listRDD = spark.makeRDD(1 to 16, 4)
    println("缩减分区前 = " + listRDD.partitions.size)
    val coalesceRDD = listRDD.coalesce(3)
    println("缩减分区后 = " + coalesceRDD.partitions.size)
    coalesceRDD.saveAsTextFile("output")
    spark.stop()
  }

}
