package one

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}

object InitSparkContext {
   def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local").setAppName("starsst")
      val sc = new SparkContext(conf)
//      val sc = new SparkContext("local[*]", "appName")

   }
}
