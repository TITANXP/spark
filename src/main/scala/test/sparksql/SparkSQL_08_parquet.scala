package test.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQL_08_parquet{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparksql08")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val rdd = spark.sparkContext.makeRDD(List((1,"a"), (2,"b"), (3, "c")))
    val df = rdd.toDF("id", "name")
    //保存为parquet文件
    df.write.mode("ignore")save("out/parquet/user.parquet")
//    df.write.format("parquet").save("out/parquet/user.parquet") 等价

    spark.stop()
  }
}
