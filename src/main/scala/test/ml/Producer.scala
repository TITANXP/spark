package test.ml
import java.io.PrintWriter
import java.net.ServerSocket

import breeze.linalg._

import scala.util.Random

/**
 * 随机线性回归数据生成器
 */
object Producer {
  def main(args: Array[String]): Unit = {
    val MaxEvents = 50
    val NumFeatures = 10

    val random = new Random()

    /**
     * 生成符合正态分布的稠密向量
     */
    def generateRandomArray(n: Int) =
      // 返回指定长度的数组，每个元素为给定函数的返回值
      Array.tabulate(n)(_ => random.nextGaussian())


    // 生成w 和 b
    val w = new DenseVector(generateRandomArray(NumFeatures))
    val intercept = random.nextGaussian()*10

    /**
     * 生成数据
     */
    def generateData(n: Int) = {
      (1 to n).map { i =>
        val x = new DenseVector(generateRandomArray(NumFeatures))
        val y = x.dot(w) + intercept
        (y, x)
      }
    }

      val socket = new ServerSocket(7789)
      println("监听端口7789...")
      while(true){
        val client = socket.accept()
        new Thread(){
          override def run() = {
            println(client.getInetAddress + "已连接")
            val out = new PrintWriter(client.getOutputStream(), true)
            while(true){
              Thread.sleep(500)
              val num = random.nextInt(MaxEvents)
              val data = generateData(num)
              data.foreach{ case(y, x) =>
                  val xStr = x.data.mkString(",")
                  val eventStr = s"$y\t$xStr"
                  out.write(eventStr)
                  out.write("\n")
              }
              out.flush()
              println(s"已发送$num 个event")
            }
            out.close()
          }
        }.start()
      }
    }

}
