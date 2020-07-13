package test.sparkstreamin

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_01_WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming01")
    val ssc = new StreamingContext(conf, Seconds(3))
    //从指定端口采集数据
    val socketLineStreaming: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 7788)
    //将采集的数据进行扁平化
    val wordDStream = socketLineStreaming.flatMap(line => line.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()
    //启动采集器
    ssc.start()
    //Driver等待采集器停止
    ssc.awaitTermination()
  }
}
