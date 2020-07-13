package test.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_05_UDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparksql05")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //创建数据集
    val rdd = spark.sparkContext.makeRDD(List((1,"a"), (2,"b"), (3,"c")))
    val dataFrame = rdd.toDF("id", "name")
    //注册用户自定义函数
    spark.udf.register("addName", (x:String) => x+"name")
    //创建临时表
    dataFrame.createTempView("user")
    //用sql查询
    spark.sql("select addName(name) from user").show()
    spark.stop()
  }
}
