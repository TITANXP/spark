package one

import org.apache.spark.SparkContext

/**
 * 创建RDD的三种方式
 */
object CreateRDD {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "createRDD")
    //从内存中创建，底层调用的parallelize
    val rdd1 = sc.makeRDD(List(1,2,3))
    val rdd2 = sc.parallelize(List(1,2,3))
    //从文件创建 可以为本地路径，也可以为hdfs路径
//    val rdd3 = sc.textFile("\in")
  }
}
