package test.spark.cacheAndCheckpoint

import org.apache.spark.SparkContext

//设置检查点
object CheckPointTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "checkpoint")
    spark.setCheckpointDir("checkpoin/")
    spark.setLogLevel("WARN")
    val rdd = spark.makeRDD(Array(("a", "a")))
      .reduceByKey(_+_)
      .map(_._2+System.currentTimeMillis())
    rdd.checkpoint()
    println("checkpoint前\n", rdd.toDebugString)
    rdd.collect().foreach(println) //调用action后才会触发checkpoint
    println("checkpoint后\n", rdd.toDebugString)

    rdd.collect().foreach(println)
    rdd.collect().foreach(println)
    spark.stop()
  }
}
