package test.sparkstreamin

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 有状态的流计算
 */
object UpdateStateByKeyApp {
  // currentTotal为Option，因为他可能是空的（第一批数据）
  def updateState(prices: Seq[(String, Double)], currentTotal: Option[(Int, Double)]) ={
    val currentRevenue = prices.map(_._2).sum
    val currentNumberPurchases = prices.size
    // 设置默认值
    val state = currentTotal.getOrElse((0, 0.0))
    Some(currentNumberPurchases + state._1, currentRevenue + state._2)
  }

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[3]", "updataStateBykey app", Seconds(2))

    //对有状态的计算，需要设置一个检查点
    ssc.checkpoint("temp/updateState")
    val stream = ssc.socketTextStream("localhost", 7788)

    val events = stream.map{record =>
        val event = record.split(",")
       (event(0), event(1), event(2).toDouble)
    }

    val users = events.map{
      case (user, product, price) =>
        (user, (product, price))
    }

    val revenuePerUser = users.updateStateByKey(updateState)
    revenuePerUser.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
