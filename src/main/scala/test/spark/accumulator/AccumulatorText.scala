package test.spark.accumulator

import org.apache.spark.SparkContext

//累加器
object AccumulatorText {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "acc")
    //创建累加器
    val accumulator = spark.longAccumulator
    val rdd = spark.makeRDD(List(1,2,3,4), 2)
    rdd.foreach{
          //执行累加器的累加功能
        case i => accumulator.add(i)
    }
    //获取累加器的值
    println(accumulator.value)
    spark.stop()
  }
}
