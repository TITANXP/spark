package test.breeze

import breeze.linalg.DenseMatrix

object test01 {
  def main(args: Array[String]): Unit = {
//    密集矩阵
    val a = DenseMatrix((1,2), (3,4))
    println(a.cols)
    println(a)
  }
}
