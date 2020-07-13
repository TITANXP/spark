package test.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//sql风格的语法
object SparkSQL_02_sqlStyle {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparksql02")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val dataFrame = spark.read.json("in/people.json")
    //对dataFrame创建一个临时表
    dataFrame.createTempView("user")
    //注意：临时表是session范围内的，session退出后，表就失效了；如果想在应用范围内有效，可以使用全局表。
    //      使用全局表需要全路径访问：如global_temp.user
    //dataFrame.createGlobalTempView("user")
    //通过SQL查询表
    val sqlDF = spark.sql("select * from user")
    //展示数据
    sqlDF.show()
    spark.stop()
  }
}
