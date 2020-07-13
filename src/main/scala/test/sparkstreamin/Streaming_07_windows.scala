package test.sparkstreamin

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
//各种window操作
object Streaming_07_windows {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("s07")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.sparkContext.setCheckpointDir("checkpoint")
    val rddQueue = new mutable.Queue[RDD[String]]
    val dStream = ssc.queueStream(rddQueue, oneAtATime = false)

    //处理（wordCount）
    //1.window
    //窗口大小应该为采集周期的整数倍，窗口滑动步长也应该是采集周期的整数倍
    dStream.map((_,1))
      .window(Seconds(4), Seconds(2))
      .reduceByKey(_+_)
    //        .print() //(a,2) (b,2)

    //2.reduceByWindow
    dStream.map((_,1))
      .reduceByWindow((a, b) => (a._1, (a._2 + b._2)), Seconds(4), Seconds(2)) //((String, Int),(String,Int)) => (String, Int)
    //        .print() //(a,4) 不管key相不相同，都被合并了
    //3.reduceByWindow 逆函数
    dStream.map((_,1))
      .reduceByWindow((a,b) => (a._1, a._2+b._2), (a,b) => (a._1, a._2-b._2), Seconds(4), Seconds(2))
//      .print() //(a,4)

    //4.reduceByKeyAndWindow
    dStream.map((_,1))
        .reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(4), Seconds(2))
//        .print() //(a,2) (b,2)
    //5.reduceByKeyAndWindow 逆函数
    dStream.map((_,1))
      .reduceByKeyAndWindow(_+_, _-_, Seconds(4), Seconds(2)) //窗口长度2个批次，间隔1个批次
    //      .print()   //(a,2) (b,2)

    //6.countByWindow
    dStream.map((_,1))
        .countByWindow(Seconds(4), Seconds(2))
//        .print() // 4

    //7.countByValueAndWindow
    dStream.map((_,1))
        .countByValueAndWindow(Seconds(4), Seconds(2))
//        .print() //((a,1),2) ((b,1),2)

    ssc.start()

    //制造数据 没两秒发送一次 a和b
    for(i <- 1 to 30){
      rddQueue += ssc.sparkContext.makeRDD(List("a", "b"))
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }
}
