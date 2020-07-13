package test.sparkstreamin

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
//Kafka数据源
object Streaming_05_KafkaSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("streaming05").setMaster("local[*]").set("spark.yarn.user.classpath.first", "true")
    val ssc = new StreamingContext(conf, Seconds(5))
    //从Kafka采集数据
    var kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "192.168.170.129:2181", "test1", Map("test1" -> 1))
    //wordCount
    //扁平化后，按照空格切分
//    kafkaDStream.print()
    var wordDstream: DStream[String] = kafkaDStream.flatMap(t => t._2.split(" "))
    var mapDstream: DStream[(String, Int)] = wordDstream.map((_, 1))
    var wordToSumDstream: DStream[(String, Int)] = mapDstream.reduceByKey(_ + _)
    wordToSumDstream.print()

    //启动采集器
    ssc.start()
    //Driver等待采集器停止
    ssc.awaitTermination()
  }
}
