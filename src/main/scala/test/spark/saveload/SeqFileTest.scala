package test.spark.saveload

import org.apache.spark.SparkContext
//SequenceFile的读取和保存
object SeqFileTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "seq")
    //创建一个RDD
    val rdd = spark.makeRDD(List((1,1), (2,2), (3,3)))
    //将RDD保存为SequenceFile
    rdd.saveAsSequenceFile("out/sequence1")

    //读取SequenceFile
    val seq = spark.sequenceFile[Int,Int]("out/sequence1")
      .collect().foreach(println)
    spark.stop()
  }
}
