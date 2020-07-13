package test.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator
//强类型UDAF
object SparkSQL_07_UDAF2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparksql07")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //创建数据
    val rdd = spark.sparkContext.makeRDD(List((1,"a"), (2,"b")))
    val df = rdd.toDF("id", "name")
    val ds = df.as[User]

    //1.创建UDAF对象
    val udf = new MyAvg2
    //强类型的UDAF不能在sql中使用
    //2.将UDAF转换为查询列，因为传入的是对象
    var avgcCol: TypedColumn[User, Double] = udf.toColumn.name("myavg")
    //3.使用
    ds.select(avgcCol).show()
    spark.stop()


  }
}

//case class User(id: Int, name: String)

//这里输入 数据类型 需要改为BigInt，不能为Int。因为程序读取文件的时候，不能判断int类型到底多大，所以会报错 truncate
case class AvgBuffer(var sum: BigInt, var count: Int)

class MyAvg2 extends Aggregator[User, AvgBuffer, Double]{//分别为输入类型，中间计算类型，返回类型
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //聚合数据
  override def reduce(b: AvgBuffer, a: User): AvgBuffer = {
    b.sum += a.id
    b.count += 1
    b
  }

  //缓冲区合并
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  //计算逻辑
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count.toDouble
  }

  //数据类型转码，自定义类型 基本都是Encoders.product
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  //基本数据类型：Encoders.scala。。。
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
