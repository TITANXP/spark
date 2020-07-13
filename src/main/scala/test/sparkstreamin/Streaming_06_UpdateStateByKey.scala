package test.sparkstreamin

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Streaming_06_UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("s06")
    val ssc = new StreamingContext(conf, Seconds(5))
    //保存状态，设置检查的路径。（如果不设置会报错)
    ssc.sparkContext.setCheckpointDir("checkpoint")
    //使用RDD队列作为数据源
    val rddQueue = new mutable.Queue[RDD[String]]()
    val dStream = ssc.queueStream(rddQueue, oneAtATime = false)
    //处理
    var mapDStream = dStream.map((_,1))
    var stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey{
        case(seq, buffer) => {
          var sum = buffer.getOrElse(0) + seq.sum //以前的数量 + 当前新增的数量
          Option(sum)
        }
      }
      stateDStream.print()
    ssc.start()
    //向RDD队列中不断添加数据
    for(i <- 1 to 30){
      rddQueue += ssc.sparkContext.makeRDD(List("a", "b"))
      Thread.sleep(3000)
    }
    ssc.awaitTermination()
  }

}
