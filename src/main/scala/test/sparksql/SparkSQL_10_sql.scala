package test.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_10_sql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("10")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val df = spark.sql("select * from parquet.`out/parquet/user.parquet`")
    df.show()
    spark.stop()
  }
}
