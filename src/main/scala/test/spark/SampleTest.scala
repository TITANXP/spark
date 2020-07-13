package test.spark

import org.apache.spark.{SparkConf, SparkContext}

object SampleTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("sample")
    val spark = new SparkContext(conf)
    val listRdd = spark.makeRDD(1 to 10)
    val sampleRDD = listRdd.sample(true, 1)
    sampleRDD.collect().foreach(println)
    spark.stop()

  }
}
