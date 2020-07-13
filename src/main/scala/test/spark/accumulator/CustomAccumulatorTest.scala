package test.spark.accumulator

import org.apache.spark.SparkContext

object CustomAccumulatorTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "custom")
    val rdd = spark.makeRDD(List("a","b","c","d"), 2)
    //创建累加器
    val accumulator = new CustomAccumulator()
    //注册累加器
    spark.register(accumulator)

    rdd.foreach{
      case s => {
        accumulator.add(s)
      }
    }
    println(accumulator.value)
    spark.stop()
  }

}
