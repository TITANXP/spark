package test.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_03_DSLStyle {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparksql03")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val dataFrame = spark.read.json("in/people.json")
    //查看DataFrame的schema信息
    dataFrame.printSchema()
    //查看name列
    dataFrame.select("name").show()
    //查看name以及age+1
    import spark.implicits._ //需要导入才能识别$, 这里的spark是变量的名字
    dataFrame.select($"name", $"age"+1).show()
    //查看age大于10的数据
    dataFrame.filter($"age" > 10).show()
    //按照age分组，查看数据条数
    dataFrame.groupBy("age").count().show()
    spark.stop()
  }
}
