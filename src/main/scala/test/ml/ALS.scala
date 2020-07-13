package test.ml

import org.apache.commons.math3.linear._
import org.apache.spark.SparkContext

object ALS {
  var movies = 0
  var users = 0
  var features = 0
  var ITERATIONS = 0 //迭代次数
  val LAMBDA = 0.01 //正则化因子


  /**
   * 生成一个n维向量，并填充随机数
   */
  private def vectory(n: Int): RealVector =
    new ArrayRealVector(Array.fill(n)(math.random))

  /**
   * 生成一个矩阵，并填充随机数
   */
  def matrix(row: Int, col: Int): RealMatrix = {
    new Array2DRowRealMatrix(Array.fill(row, col)(math.random))
  }

  def rSpace(): RealMatrix = {
    val mh = matrix(movies, features)
    val uh = matrix(users, features)
    return mh.multiply(uh.transpose())
  }

  /**
   * 求均方根误差
   */
  def rmse(targetR: RealMatrix, ms: Array[RealVector], us: Array[RealVector]): Double = {
    val r = new Array2DRowRealMatrix(movies, users)
    for (i <- 0 until movies; j <- 0 until users){
      // 点乘
      r.setEntry(i, j, ms(i).dotProduct(us(j)))
    }
    //减
    val diffs = r.subtract(targetR)
    var sumSqs = 0.0
    for (i <- 0 until movies; j <- 0 until users) {
      val diff = diffs.getEntry(i, j)
      sumSqs += diff * diff
    }
    math.sqrt(sumSqs / (movies.toDouble * users.toDouble))
  }

  def update(i: Int, m: RealVector, us: Array[RealVector], R: RealMatrix): RealVector = {
    val U = us.length
    val F = us(0).getDimension
    var XtX: RealMatrix = new Array2DRowRealMatrix(F, F)
    var Xty: RealVector = new ArrayRealVector(F)
    for(j <- 0 until U) {
      val u = us(j)
      //u*u^t
      XtX = XtX.add(u.outerProduct(u))
      Xty = Xty.add(u.mapMultiply(R.getEntry(i,j)))
      println(XtX)
      println(Xty)
      println("----------------------")
    }
    //给对角线加上正则化因子
    for(d <- 0 until F){
      XtX.addToEntry(d, d, LAMBDA * U)
    }
    new CholeskyDecomposition(XtX).getSolver.solve(Xty)
  }

  def main(args: Array[String]): Unit = {
    //随机初始化变量
    movies = 10
    users = 5
    features = 3
    ITERATIONS = 5
    var silces = 1

    //初始化sparkContext
    val sc = new SparkContext("local", "ALS")

    //得到真实值矩阵
    val r_space = rSpace()

    // 随机初始化ms us
    var ms = Array.fill(movies)(vectory(features))
    var us = Array.fill(users)(vectory(features))

    // 迭代更新电影和用户矩阵
    val Rc = sc.broadcast(r_space)
    var msb = sc.broadcast(ms)
    var usb = sc.broadcast(us)

    for(i <- 1 to  ITERATIONS){
      println(s"迭代次数： $i:")
      ms = sc.parallelize(0 until movies, silces)
          .map(i => update(i, msb.value(i), usb.value, Rc.value))
          .collect()
      msb = sc.broadcast(ms)
      println(ms.toList)

      us = sc.parallelize(0 until users, silces)
          .map(i => update(i, usb.value(i), msb.value, Rc.value.transpose()))
          .collect()
      usb = sc.broadcast(us)
      println("误差：" + rmse(r_space, ms, us))
    }
    sc.stop()
  }
}
