package test.spark.partition

import org.apache.spark.{HashPartitioner, RangePartitioner, SparkContext}

object HashAndRangePartitionTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "hashpart")
    spark.setLogLevel("ERROR")
    val rdd = spark.makeRDD(List((1,1),(1,2),(1,3),(2,1),(2,2),(3,1)), 3)
    println("没有使用分区器")
    rdd.mapPartitionsWithIndex(
      (index, iter) => {
        Iterator(index.toString + ":" + iter.mkString("|"))
      }
    ).collect().foreach(println)

    println("使用hash分区器")
    val hashRdd = rdd.partitionBy(new HashPartitioner(3))
    val rangeRdd = rdd.partitionBy(new RangePartitioner(3, rdd, true))
    println(hashRdd.partitioner)
    hashRdd.mapPartitionsWithIndex(
      (index, iter) => {
        Iterator(index.toString + ":" + iter.mkString("|"))
      }
    ).collect().foreach(println)

    println("使用range分区器")
    println(rangeRdd.partitioner)
    rangeRdd.mapPartitionsWithIndex(
      (index, iter) => {
        Iterator(index.toString + ":" + iter.mkString("|"))
      }
    ).collect().foreach(println)
    spark.stop()
  }
}
