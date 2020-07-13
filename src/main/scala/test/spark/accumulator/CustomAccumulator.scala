package test.spark.accumulator

import java.util

import org.apache.spark.util.AccumulatorV2
//自定义累加器
class CustomAccumulator extends AccumulatorV2[String, util.ArrayList[String]]{
  //用来存放单词
  private val strSet = new util.ArrayList[String]();

  //当前累加器是否为初始化状态
  override def isZero: Boolean = {
    strSet.isEmpty
  }

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    val newAcc = new CustomAccumulator()
    strSet.synchronized{
      newAcc.strSet.addAll(strSet)
    }
    newAcc
  }

  //重置累加器对象
  override def reset(): Unit = {
    strSet.clear()
  }

  //向累加器中添加数据
  override def add(v: String): Unit = {
    strSet.add(v)
  }

  //合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    strSet.addAll(other.value)
  }

  //返回累加器的值
  override def value: util.ArrayList[String] = strSet
}
