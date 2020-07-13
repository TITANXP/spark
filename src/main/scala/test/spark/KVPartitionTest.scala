package test.spark

import org.apache.spark.{Partitioner, SparkContext}
//分区器
object KVPartitionTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "par")
    //KV RDD
    val rdd = spark.parallelize(List(("a", 1), ("b", 2)))
      .partitionBy(new MyPartitioner(2))
      .saveAsTextFile("output")
  }
}

class MyPartitioner(partitions: Int) extends Partitioner{
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {
    1 //都放在第分区1
  }
}
