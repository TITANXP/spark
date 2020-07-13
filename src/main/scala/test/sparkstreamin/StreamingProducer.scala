package test.sparkstreamin


import java.io.PrintWriter
import java.net.ServerSocket

import scala.util.Random

object StreamingProducer {
  def main(args: Array[String]): Unit = {
    val random = new Random()

    // 每秒最大活动数
    val MaxEvents = 6

    val nameResource = this.getClass.getResourceAsStream("name.csv")

    // 读取姓名列表
    val names = scala.io.Source.fromInputStream(nameResource)
      .getLines()
      .toList
      .head
      .split(",")
      .toSeq

    println(names.getClass)
    println(names)

    // 生成产品
    val products = Seq(
      "iphone" -> 9.99,
      "sony" -> 8.88,
      "samsung" -> 9.00,
      "ipad" -> 9.11
    )

    // 生成随机产品活动
    def generateProductEvents(n: Int) = {
      (1 to n).map { i =>
        val (product, price) = products(random.nextInt(products.size))
        val user = random.shuffle(names).head
        (user, product, price)
      }
    }

    val socket = new ServerSocket(7788)
    println("监听端口7788, 等待连接...")
    while(true) {
      val client = socket.accept()
      println(socket.getInetAddress + " 以连接")
      new Thread(){
        override def run() ={
          val out = new PrintWriter(client.getOutputStream(), true)
          while(true) {
            Thread.sleep(1000)
            val num = random.nextInt(MaxEvents)
            val productEvents = generateProductEvents(num)

            productEvents.foreach{event =>
              out.write(event.productIterator.mkString(","))
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
