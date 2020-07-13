package test.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
//RDD、DS、DF的转换
object SparkSQL_04_transform {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark04")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val rdd: RDD[(Int, String)]  = spark.sparkContext.makeRDD(List((1,"a"), (2,"b")))

    //进行转换前，需要导入隐式转换规则
    import spark.implicits._
    //RDD转DF
    var df: DataFrame = rdd.toDF("id", "name")
    //DF转DS
    var ds: Dataset[User] = df.as[User]
    //DS转DF
    var df1: DataFrame = ds.toDF()
    //DF转RDD
    var rdd1: RDD[Row] = df.rdd
    //DS转RDD
    var rdd2: RDD[User] = ds.rdd
    //RDD转DS
    var ds2: Dataset[User] = rdd.map{
      line => User(line._1, line._2)
    }.toDS()

    rdd1.foreach(
      //获取数据时，可以通过索引访问数据
      row => println(row.getString(1))
    )

    df.foreach{
      line => {
        val id = line.getAs[Int]("id")
        val name = line.getAs[String]("name")
      }
    }
    df.map{
      case Row(id: Int, name: String) => id
    }
//    ds.map{
//      case Row(col1: Int, col2: String) => println(col1+col2)
//    }

//    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> "out/user.csv")
//    df.write.format("out/user.csv")
//        .mode(SaveMode.Overwrite)
//        .options(saveOptions)
//        .save()

    spark.stop()
  }
}

//DataSet需要的样例类
case class User(id: Int, name: String);
