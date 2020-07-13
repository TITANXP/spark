package test.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

//spark调用其它类的方法时，需要实现序列化接口
object SerializableTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "ser")
    val rdd = spark.makeRDD(List("aa", "ba", "c"))
    val search = new Search("a")
    val rdd1 = search.getMatch2(rdd)
      .collect()
      .foreach(println)
    spark.stop()
  }

}

class Search(query: String){
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatch2(rdd: RDD[String]): RDD[String] = {
    val q = query;
    rdd.filter(_.contains(q))
  }
}
