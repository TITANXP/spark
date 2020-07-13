package test.sparkstreamin

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingTest01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("streaming").setMaster("local[2]")
//    从sparkconf创建StreamingCntext，并指定1秒的批处理大小
    val ssc = new StreamingContext(conf, Seconds(2))
    val lines = ssc.socketTextStream("192.168.170.129", 7777).flatMap(_.split(" "))

//    val erroelines = lines.filter(_.contains("error"))


    val windows = lines.window(Seconds(4), Seconds(2))

    println(windows.print())

    ssc.start()
    ssc.awaitTermination()
  //    在linux上 nc -lk 7777
  }

}
