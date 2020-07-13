package test.spark.accumulator

import org.apache.spark.SparkContext
//广播变量
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val spark = new SparkContext("local[*]", "broad")
    val rdd = spark.makeRDD(List((1,"a"), (2, "b"), (3, "c")))
    //创建广播变量
    val broadcast = spark.broadcast(List((1,1),(2,2),(3,3)))
    rdd.map{
      case(key, value) => {
        var v2: Any = null;
        for(t <- broadcast.value){
          if(key == t._1)
              v2 = t._2
        }
        (key, (value, v2))
      }
    }
      .collect().foreach(println)
    spark.stop()
  }
}
