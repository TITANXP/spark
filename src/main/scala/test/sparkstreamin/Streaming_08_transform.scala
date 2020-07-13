package test.sparkstreamin

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Streaming_08_transform {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("s08")
    val ssc = new StreamingContext(conf, Seconds(2))
    var rddQueue = new mutable.Queue[RDD[String]]
    val dStream = ssc.queueStream(rddQueue, oneAtATime = false)
    //处理
    dStream.map { //String => U
      case x =>{
        x
      }
    }

    //写在这里的代码，在Driver端执行，只执行一次
    dStream.transform { //RDD[String] => RDD[U]
      rdd => {
        // 写在这里的代码，每个采集周期（当前为4s）执行一次
        println("111111")
        rdd.map{
          case x => {
            //写在这里的代码，在每个Executer执行
            println("executer")
            x
          }
        }
      }
    }.print()

    ssc.start()
    for(i <- 1 to 30){
      rddQueue += ssc.sparkContext.makeRDD(List("a", "b"))
    }
    ssc.awaitTermination()
  }

}
