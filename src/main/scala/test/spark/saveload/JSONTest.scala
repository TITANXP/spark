package test.spark.saveload

import org.apache.spark.SparkContext
//导入解析JSON所需的包
import scala.util.parsing.json.JSON

object JSONTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "json")
    //将JSON文件当作TextFile读取
    val rdd = spark.textFile("in/people.json")
    rdd.collect().foreach(println)
//    {"name": "a", "age" : 10}
//    {"name": "b", "age" : 11}
//    {"name": "c", "age" : 12}

    //解析JSON
    rdd.map(JSON.parseFull).collect().foreach(println)
//    Some(Map(name -> a, age -> 10.0))
//    Some(Map(name -> b, age -> 11.0))
//    Some(Map(name -> c, age -> 12.0))
    spark.stop()
  }
}
