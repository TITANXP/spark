package test.spark.partition

import org.apache.spark.{HashPartitioner, SparkContext}

object PartitionTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "partition")
    //创建一个pairRDD
    val pairRDD = spark.parallelize(List((1,1), (2,2), (3,3)))
    println(pairRDD.partitioner) //None
    //s使用HashPartition对RDD重新分区
    val partitioned = pairRDD.partitionBy(new HashPartitioner(2))
    println(partitioned.partitioner) //Some(org.apache.spark.HashPartitioner@2)
    spark.stop();
  }
}
