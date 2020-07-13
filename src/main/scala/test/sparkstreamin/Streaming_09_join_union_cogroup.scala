package test.sparkstreamin

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Streaming_09_join_union_cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("s09")
    val ssc = new StreamingContext(conf, Seconds(2))
    val rddQueue1 = new mutable.Queue[RDD[String]]
    val rddQueue2 = new mutable.Queue[RDD[String]]
    val stream1 = ssc.queueStream(rddQueue1, oneAtATime = false).map((1,_))
    val stream2 = ssc.queueStream(rddQueue2, oneAtATime = false).map((1,_))
    //k，v类型才能使用join
    stream1.join(stream2)
    //   .print()
    // (1,(a,b)) (1,(a,b))

    stream1.union(stream2)
        .print()
    //(1,a)
    //(1,a)
    //(1,b)

    stream1.cogroup(stream2)
    //    .print()
    //(1,(CompactBuffer(a, a),CompactBuffer(b)))

    ssc.start()
    //制造数据
    for(i <- 1 to 30){
      rddQueue1 += ssc.sparkContext.makeRDD(List("a", "a"))
      rddQueue2 += ssc.sparkContext.makeRDD(List("b"))
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }
}
