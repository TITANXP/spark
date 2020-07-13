package test.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
//用户自定义聚合函数
object SparkSQL_05_UDAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparksql05")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //创建数据
    val rdd = spark.sparkContext.makeRDD(List((1,"a"), (2,"b")))
    val df = rdd.toDF("id", "name")
    //创建UDAF对象
    val udf = new MyAvg
    //注册函数
    spark.udf.register("myavg", udf)
    //使用UDAF
    df.createTempView("user")
    spark.sql("select myavg(id) from user").show()
    spark.stop()


  }

}

//UDAF
class MyAvg extends UserDefinedAggregateFunction{
  //函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType()
      .add("sum", LongType)
      .add("count", LongType)
  }

  //函数返回的数据类型
  override def dataType: DataType = DoubleType

  //函数是否稳定: 给相同的值，在不同的时间，结果是否一致
  override def deterministic: Boolean = true

  //计算前缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //没有名称，只有结构，只能通过标记位来确定
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //根据查询结果，更新数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //将多个节点的缓冲区进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算逻辑
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1).toDouble
  }
}
