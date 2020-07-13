package test.sparkstreamin

import java.io.{BufferedInputStream, BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable

//自定义采集器
object Streaming_04_MyReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming04")
    val ssc = new StreamingContext(conf, Seconds(3))
    var receiver = ssc.receiverStream(new MyReceiver("localhost", 7788))
    receiver.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

//采集器
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  var socket: Socket = null

  def receiver(): Unit = {
    socket = new Socket(host, port)
    var bufferInputStream = new BufferedReader(new InputStreamReader(socket.getInputStream()))
    var line: String = null
    while((line = bufferInputStream.readLine()) != null){
      //将数据存储到采集器的内部
      if("end".equals(line))
        return
      this.store(line)
    }
  }
  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receiver()
      }
    }).start()
  }

  override def onStop(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        if(socket != null) {
          socket.close()
          socket = null
        }
      }
    }).start()
  }
}

