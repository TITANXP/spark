package test.spark

import org.apache.spark.SparkContext

object LineagerTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "l")
    val words = spark.makeRDD(List("a", "a", "b"))
    val wordToOne = words.map((_, 1))
    val wordToSum = wordToOne.reduceByKey(_+_)
    println("查看wordToOne的Lineage\n", wordToOne.toDebugString)
    println("查看wordToSum的Lineage\n", wordToSum.toDebugString)
    println("wordToOne的依赖类型\n", wordToOne.dependencies)
    println("wordToSum的依赖类型\n", wordToSum.dependencies)
    spark.stop()
  }
}
