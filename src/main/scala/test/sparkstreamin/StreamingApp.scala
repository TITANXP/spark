package test.sparkstreamin

import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingApp {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[2]", "streaming app", Seconds(1))
    val stream = ssc.socketTextStream("localhost", 7788)
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
