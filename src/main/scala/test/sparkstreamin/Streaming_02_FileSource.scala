package test.sparkstreamin

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

//将文件作为数据源
object Streaming_02_FileSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("s02")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")
    //从文件夹中采集数据，并作wordcount
    ssc.textFileStream("in/fileSource")
      .flatMap(line => line.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()
    //启动采集器
    ssc.start()
    //Driver等待采集器停止
    ssc.awaitTermination()
  }
}
