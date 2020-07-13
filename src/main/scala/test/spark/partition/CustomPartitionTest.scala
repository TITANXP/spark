package test.spark.partition

import org.apache.spark.{Partitioner, SparkContext}

//自定义分区器
//将有相同后缀的数据分入到同一个分区
object CustomPartitionTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "cu")
    val rdd = spark.makeRDD(List((1,1), (2,2), (3,3),(11,11), (12,12), (13,13)))
    rdd.partitionBy(new CustomPartition(3))
      .mapPartitionsWithIndex(
        (index, iter) => {

          Iterator(index + ":" + iter.mkString("|"))
        }
      ).collect().foreach(println)
    spark.stop()
  }
}

class CustomPartition(numPart: Int) extends Partitioner{
  //分区数
  override def numPartitions: Int = numPart
  //获取key对应的分区号
  override def getPartition(key: Any): Int = {
    val keyStr = key.toString()
    //分区号为key的最后一位与分区号取余
    keyStr.substring(keyStr.length-1).toInt % numPartitions
  }
}
